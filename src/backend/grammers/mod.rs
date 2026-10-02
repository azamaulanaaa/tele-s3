use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use grammers_client::{
    Client,
    media::{Media, Uploaded},
    message::InputMessage,
    tl::{functions::upload::SaveBigFilePart, types::InputFileBig},
};
use grammers_mtsender::{InvocationError, SenderPool, SenderPoolFatHandle};
use grammers_session::types::PeerRef;
use sea_orm::DatabaseConnection;
use tokio::{
    io::AsyncReadExt,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt},
    io::StreamReader,
};
use tracing::{instrument, warn};

use super::{Backend, BackendError, BoxedAsyncReader};

mod session;

const PART_SIZE: usize = 512 * 1024;
const MAX_PARTS: i32 = 4000;
// Telegram file chunk size used for downloads (mirrors grammers'
/// `client::files::MAX_CHUNK_SIZE`, which is private in 0.10).
const MAX_CHUNK_SIZE: i32 = 512 * 1024;

// Error code Telegram uses for every timed rate-limit variant
// (`FLOOD_WAIT_X`, `FLOOD_PREMIUM_WAIT_X`, ...).
const FLOOD_ERROR_CODE: i32 = 420;

// Smallest deadline accepted from configuration. A zero timeout would abort
// every request the instant it started, which looks like a total outage
// rather than a misconfiguration.
const MIN_IO_TIMEOUT: Duration = Duration::from_secs(1);

/// Telegram operations allowed in flight at once, process-wide.
///
/// The S3 layer fans out one backend call per blob (`DeleteObjects`, a
/// multi-part GET), and Telegram rate-limits a burst aimed at a single chat,
/// so the bound is applied here, at the backend boundary, where every caller
/// inherits it.
pub const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 8;

/// Deadline for a single Telegram request, generous enough for a
/// `PART_SIZE`/`MAX_CHUNK_SIZE` transfer on a slow link.
pub const DEFAULT_IO_TIMEOUT: Duration = Duration::from_secs(120);

/// Attempts per retry loop, the first try included.
pub const DEFAULT_MAX_ATTEMPTS: u32 = 8;

type BoxedStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>;

/// Resource bounds applied to every outbound Telegram request.
///
/// The defaults keep a single-object workload behaving as it did before —
/// nothing was in flight concurrently in that shape, and no request took
/// anywhere near two minutes — while stopping an unbounded fan-out from
/// opening a burst and a wedged RPC from pinning a request forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrammersLimits {
    /// Maximum number of Telegram operations in flight at once. Zero or an
    /// absurd value is clamped by [`GrammersLimits::sanitized`].
    pub max_concurrent_requests: usize,
    /// Deadline for one request, counted from the moment its concurrency
    /// permit is held (queueing behind other callers is not charged to it).
    pub io_timeout: Duration,
    /// Maximum attempts per retry loop, the first try included.
    pub max_attempts: u32,
}

impl Default for GrammersLimits {
    fn default() -> Self {
        Self {
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            io_timeout: DEFAULT_IO_TIMEOUT,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

impl GrammersLimits {
    /// Clamp operator-supplied values into ranges that cannot wedge or
    /// panic the runtime: zero permits or a zero deadline would stall every
    /// request forever, and [`Semaphore::new`] panics above
    /// [`Semaphore::MAX_PERMITS`].
    pub fn sanitized(mut self) -> Self {
        self.max_concurrent_requests = self
            .max_concurrent_requests
            .clamp(1, Semaphore::MAX_PERMITS);
        self.io_timeout = self.io_timeout.max(MIN_IO_TIMEOUT);
        self.max_attempts = self.max_attempts.max(1);

        self
    }
}

pub struct GrammersConfig {
    pub app_id: i32,
    pub app_hash: String,
    pub bot_token: String,
    pub db: DatabaseConnection,
    pub username: String,
    /// Outbound concurrency bound, per-request deadline and retry budget.
    pub limits: GrammersLimits,
}

#[derive(Clone)]
pub struct Grammers {
    client: Client,
    sender_pool_handle: SenderPoolFatHandle,
    peer: PeerRef,
    flood_guard: Arc<Mutex<Option<Instant>>>,
    /// Bounds how many Telegram operations may be in flight at once, however
    /// many callers the layer above fans out.
    inflight: Arc<Semaphore>,
    limits: GrammersLimits,
}

/// What a failed Telegram RPC means for the calling retry loop.
#[derive(Debug, PartialEq, Eq)]
enum RpcFailure {
    /// A timed rate limit: sleep `Duration` and try the same request again.
    FloodWait(Duration),
    /// A rate-limit variant carrying no duration; it cannot be waited out
    /// here, so the request fails.
    FloodWithoutDuration { name: String },
    /// Not a rate limit; the request fails.
    Fatal,
}

/// Classify a server-reported RPC error.
///
/// `value` is the integer the server appended to the error name, i.e. the
/// required wait in seconds for every timed rate-limit variant. One extra
/// second of slack keeps a retry from landing inside the same window.
/// Pure: no connection, no clock.
fn classify_rpc_error(code: i32, name: &str, value: Option<u32>) -> RpcFailure {
    if code != FLOOD_ERROR_CODE {
        return RpcFailure::Fatal;
    }

    match value {
        Some(seconds) => RpcFailure::FloodWait(Duration::from_secs(seconds as u64 + 1)),
        None => RpcFailure::FloodWithoutDuration {
            name: name.to_owned(),
        },
    }
}

/// Whether a retry loop may run another attempt.
#[derive(Debug, PartialEq, Eq)]
enum Attempt {
    /// Budget remains.
    Retry,
    /// `attempts` attempts have been made without success.
    Exhausted { attempts: u32 },
}

/// Account for the attempt that just failed. `attempt` is 1-based.
///
/// Pure: the loop feeds it its own counter, so no clock is involved.
fn next_attempt(attempt: u32, max_attempts: u32) -> Attempt {
    if attempt < max_attempts {
        Attempt::Retry
    } else {
        Attempt::Exhausted { attempts: attempt }
    }
}

/// Outcome of one outbound Telegram request, after the concurrency bound and
/// the per-request deadline have been applied.
enum AttemptError {
    /// The server (or the transport) rejected the request.
    Rpc(Box<InvocationError>),
    /// The request outlived its deadline. Not retryable: the server may
    /// still complete a request we stopped waiting for, and re-sending it
    /// is not always safe — a timed-out `send_message` would duplicate the
    /// message.
    Timeout(Duration),
}

impl AttemptError {
    fn into_backend_error(self) -> BackendError {
        match self {
            AttemptError::Rpc(err) => BackendError::Other(err),
            AttemptError::Timeout(timeout) => BackendError::Other(
                format!("Telegram request exceeded the {timeout:?} deadline").into(),
            ),
        }
    }
}

impl Grammers {
    #[instrument(skip(config), level = "debug", err)]
    pub async fn init(config: GrammersConfig) -> anyhow::Result<Self> {
        let limits = config.limits.sanitized();
        tracing::info!(
            max_concurrent_requests = limits.max_concurrent_requests,
            io_timeout = ?limits.io_timeout,
            max_attempts = limits.max_attempts,
            "Telegram backend limits"
        );

        let session = {
            let session = session::SessionStorage::init(config.db).await?;

            Arc::new(session)
        };
        let pool = SenderPool::new(session.clone(), config.app_id);
        let client = Client::new(pool.handle.clone());

        let sender_pool_handle = {
            let SenderPool {
                runner,
                handle,
                updates,
            } = pool;
            // Dropping the JoinHandle detaches the task; it keeps running.
            tokio::spawn(runner.run());
            let _ = client.stream_updates(updates, Default::default()).await;

            handle
        };

        {
            let is_authorized = client.is_authorized().await?;

            if !is_authorized {
                client
                    .bot_sign_in(&config.bot_token, &config.app_hash)
                    .await?;
            }
        };

        let peer = client
            .resolve_username(&config.username)
            .await?
            .ok_or(anyhow!("Username {} not found", config.username))?;
        let peer = peer
            .to_ref()
            .await
            .map_err(|e| anyhow!("Failed to resolve peer ref: {e}"))?
            .ok_or(anyhow!(
                "Username {} has no usable peer ref",
                config.username
            ))?;

        Ok(Self {
            sender_pool_handle,
            client,
            peer,
            flood_guard: Default::default(),
            inflight: Arc::new(Semaphore::new(limits.max_concurrent_requests)),
            limits,
        })
    }

    #[instrument(skip(self), level = "debug")]
    pub fn close(self) {
        self.sender_pool_handle.quit();
    }

    async fn check_flood_wait(&self) -> Result<(), BackendError> {
        let wait_time = {
            let guard = self
                .flood_guard
                .lock()
                .map_err(|_| BackendError::PoisonedLock {
                    lock: "flood_guard",
                })?;

            if let Some(until) = *guard {
                let now = Instant::now();
                if until > now { Some(until - now) } else { None }
            } else {
                None
            }
        };

        if let Some(duration) = wait_time {
            warn!("Flood guard active. Sleeping for {:.2?}", duration);
            tokio::time::sleep(duration).await;

            let mut guard = self
                .flood_guard
                .lock()
                .map_err(|_| BackendError::PoisonedLock {
                    lock: "flood_guard",
                })?;
            *guard = None;
        }

        Ok(())
    }

    async fn catch_flood_error(&self, err: &InvocationError) -> Result<bool, BackendError> {
        let InvocationError::Rpc(rpc_err) = err else {
            return Ok(false);
        };

        match classify_rpc_error(rpc_err.code, &rpc_err.name, rpc_err.value) {
            RpcFailure::FloodWait(duration) => {
                {
                    let mut guard =
                        self.flood_guard
                            .lock()
                            .map_err(|_| BackendError::PoisonedLock {
                                lock: "flood_guard",
                            })?;
                    *guard = Some(Instant::now() + duration);
                }

                tracing::warn!("Hit {} ({}s). Blocking.", rpc_err.name, duration.as_secs());

                Ok(true)
            }
            RpcFailure::FloodWithoutDuration { name } => Err(BackendError::Other(
                format!("{name} without duration").into(),
            )),
            RpcFailure::Fatal => Ok(false),
        }
    }

    /// Run one outbound Telegram request under both bounds.
    ///
    /// The permit is taken before the request starts and dropped as soon as
    /// the attempt ends — the request is never issued without one, so the
    /// bound holds at the wire and not just in the caller's own accounting.
    /// It is deliberately *not* held across a flood wait or a retry: a task
    /// sleeping out a rate limit must never withhold capacity from the
    /// tasks that are making progress.
    ///
    /// A task holds at most one permit and never waits for a second while
    /// holding one, so the queue can never depend on itself to drain.
    ///
    /// The deadline starts once the permit is held, so time spent queued
    /// behind other callers is never charged against a request that has not
    /// started yet.
    async fn attempt<T, F>(&self, rpc: F) -> Result<T, AttemptError>
    where
        F: Future<Output = Result<T, InvocationError>>,
    {
        let _permit: OwnedSemaphorePermit = self
            .inflight
            .clone()
            .acquire_owned()
            .await
            .expect("the concurrency limiter is never closed");

        match tokio::time::timeout(self.limits.io_timeout, rpc).await {
            Ok(result) => result.map_err(|err| AttemptError::Rpc(Box::new(err))),
            Err(_elapsed) => Err(AttemptError::Timeout(self.limits.io_timeout)),
        }
    }

    /// Fetch the chunk that holds `pos`, or `None` at the end of the media.
    ///
    /// A fresh `DownloadIter` is built per chunk: it loses its internal
    /// state after any error (its variant is replaced by `Empty`), so
    /// reusing one across errors makes `next()` return `Ok(None)` and
    /// silently truncate the stream — one instance per chunk is the only way
    /// to resume reliably.
    ///
    /// Only a long flood is retried, and only within the attempt budget, so
    /// a media that floods on every chunk fails the caller instead of
    /// looping forever. A deadline aborts the chunk: the server may still
    /// deliver it, but the iterator that would resume it is gone, and
    /// truncating the body silently would be worse than failing.
    async fn download_chunk(
        &self,
        media: &Media,
        pos: usize,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        let skip_chunks = pos / MAX_CHUNK_SIZE as usize;
        let mut attempt: u32 = 0;

        loop {
            self.check_flood_wait().await?;
            attempt += 1;

            let mut download_iter = self
                .client
                .iter_download(media)
                .chunk_size(MAX_CHUNK_SIZE)
                .skip_chunks(skip_chunks as i32);

            let outcome = self.attempt(download_iter.next()).await;

            match outcome {
                Ok(Some(chunk)) => return Ok(Some(chunk)),
                Ok(None) => return Ok(None),
                Err(AttemptError::Rpc(err)) => {
                    if !self.catch_flood_error(&err).await? {
                        return Err(AttemptError::Rpc(err).into_backend_error());
                    }

                    match next_attempt(attempt, self.limits.max_attempts) {
                        Attempt::Retry => {}
                        Attempt::Exhausted { attempts } => {
                            warn!(attempts, error = %err, "Download retry budget exhausted");
                            return Err(AttemptError::Rpc(err).into_backend_error());
                        }
                    }
                }
                Err(AttemptError::Timeout(timeout)) => {
                    warn!(?timeout, "Download chunk exceeded its deadline");
                    return Err(AttemptError::Timeout(timeout).into_backend_error());
                }
            }
        }
    }

    /// Drive `rpc` until it succeeds, a retryable rate limit clears, or the
    /// attempt budget runs out.
    ///
    /// Every attempt is preceded by [`Grammers::check_flood_wait`], so the
    /// window a 420 asked for is always slept off in full before the
    /// request goes back out. The loop is bounded: a request that keeps
    /// failing now fails its caller instead of pinning it indefinitely.
    async fn retry_rpc<T, F, Fut>(&self, mut rpc: F) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, InvocationError>>,
    {
        let mut attempt: u32 = 0;

        loop {
            self.check_flood_wait().await?;
            attempt += 1;

            let err = match self.attempt(rpc()).await {
                Ok(value) => return Ok(value),
                Err(AttemptError::Timeout(timeout)) => {
                    warn!(?timeout, "Telegram request exceeded its deadline");
                    return Err(AttemptError::Timeout(timeout).into_backend_error());
                }
                Err(AttemptError::Rpc(err)) => err,
            };

            if !self.catch_flood_error(&err).await? {
                return Err(AttemptError::Rpc(err).into_backend_error());
            }

            match next_attempt(attempt, self.limits.max_attempts) {
                Attempt::Retry => {}
                Attempt::Exhausted { attempts } => {
                    warn!(attempts, error = %err, "Telegram retry budget exhausted");
                    return Err(AttemptError::Rpc(err).into_backend_error());
                }
            }
        }
    }
}

#[async_trait]
impl Backend for Grammers {
    #[instrument(skip(self, reader), level = "debug", ret, err)]
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError> {
        let max_size = MAX_PARTS as u64 * PART_SIZE as u64;
        if size > max_size {
            return Err(BackendError::ExceedLimitSize {
                max: max_size,
                actual: size,
            });
        }

        let file_id: i64 = {
            let now = std::time::SystemTime::now();
            let duration = now
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|e| BackendError::Other(e.into()))?;
            duration.as_millis() as i64
        };

        let name = uuid::Uuid::new_v4().to_string();
        let mut compat_reader = reader.compat();

        self.check_flood_wait().await?;

        let mut part_index = 0;
        let total_parts = size.div_ceil(PART_SIZE as u64) as i32;
        let mut remaining = size as usize;
        loop {
            let mut buffer = vec![0u8; PART_SIZE.min(remaining)];
            let n = compat_reader
                .read_exact(&mut buffer)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;
            if n == 0 {
                break;
            }
            remaining -= n;

            // Retry the part through flood waits; re-uploading the same
            // part index is idempotent on Telegram's side. Each attempt
            // carries the same `file_id`/`file_part` and the same bytes.
            self.retry_rpc(|| async {
                self.client
                    .invoke(&SaveBigFilePart {
                        file_id,
                        file_part: part_index,
                        file_total_parts: total_parts,
                        // cloned so a failed attempt can be retried with
                        // the same bytes
                        bytes: buffer.clone(),
                    })
                    .await
            })
            .await?;

            part_index += 1;

            if n < PART_SIZE {
                break;
            }
        }

        let uploaded = Uploaded::from_raw(
            InputFileBig {
                id: file_id,
                parts: part_index,
                name,
            }
            .into(),
        );

        let draft_message = InputMessage::new().file(uploaded).silent(true);

        // The parts are already uploaded, so this is a single metadata RPC
        // against the peer. It is deliberately *not* retried on a deadline:
        // the message may still have been created, and sending it again
        // would publish a duplicate.
        let message = self
            .retry_rpc(|| async {
                self.client
                    .send_message(self.peer, draft_message.clone())
                    .await
            })
            .await?;
        let message_id = message.id().to_string();

        Ok(message_id)
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError> {
        let message_id = {
            let numb_key = key.parse::<i32>();

            match numb_key {
                Ok(v) => v,
                Err(_e) => {
                    return Ok(None);
                }
            }
        };
        let offset: usize = offset.try_into().map_err(|_| BackendError::OutOfRange)?;
        let limit: Option<usize> = limit
            .map(|v| v.try_into())
            .transpose()
            .map_err(|_| BackendError::OutOfRange)?;

        // A long flood wait (> grammers' sleep threshold) surfaces here as
        // an RPC error; honor it, update the shared guard, and retry so a
        // read doesn't fail just because another operation tripped the limit.
        let media = self
            .retry_rpc(|| async {
                self.client
                    .get_messages_by_id(self.peer, &[message_id])
                    .await
            })
            .await?;
        let media = {
            let mut messages = media;
            let message = match messages.pop().flatten() {
                Some(v) => v,
                None => return Ok(None),
            };

            match message.media() {
                Some(v) => v,
                None => return Ok(None),
            }
        };

        let this = self.clone();

        let stream = try_stream! {
            // Absolute position (relative to media start) of the next byte
            // to deliver.
            let mut pos = offset;
            let mut bytes_remaining = limit;

            loop {
                if bytes_remaining == Some(0) {
                    break;
                }

                // One chunk per iteration, with a fresh download iterator
                // each time; a real failure aborts the stream so the client
                // sees a failure instead of a short body.
                let chunk = this
                    .download_chunk(&media, pos)
                    .await
                    .map_err(std::io::Error::other)?;

                let Some(mut chunk) = chunk else {
                    break;
                };

                // Trim bytes below `pos` inside the first fetched chunk.
                let chunk_pos = pos % MAX_CHUNK_SIZE as usize;
                let cut = chunk_pos.min(chunk.len());
                chunk.drain(0..cut);
                if chunk.is_empty() {
                    break;
                }

                if let Some(rem) = bytes_remaining {
                    if chunk.len() > rem {
                        chunk.truncate(rem);
                    }
                    bytes_remaining = Some(rem.saturating_sub(chunk.len()));
                }

                pos += chunk.len();

                yield bytes::Bytes::from(chunk);
            }
        };

        let reader = {
            let pinned_stream: BoxedStream = Box::pin(stream);
            let reader_compat = StreamReader::new(pinned_stream).compat();

            Box::pin(reader_compat)
        };

        Ok(Some(reader))
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let message_id = {
            let numb_key = key.parse::<i32>();

            match numb_key {
                Ok(v) => v,
                Err(_e) => {
                    return Ok(());
                }
            }
        };

        // Deleting a message that is already gone succeeds server-side, so
        // every attempt carries the same request and the loop stays bounded
        // by the attempt budget.
        self.retry_rpc(|| async {
            self.client
                .delete_messages(self.peer, &[message_id])
                .await
                .map(|_deleted| ())
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flood_error_with_duration_is_retried_after_the_requested_window() {
        assert_eq!(
            classify_rpc_error(420, "FLOOD_WAIT", Some(31)),
            RpcFailure::FloodWait(Duration::from_secs(32))
        );
        // Every timed rate-limit variant shares the 420 code.
        assert_eq!(
            classify_rpc_error(420, "FLOOD_PREMIUM_WAIT", Some(600)),
            RpcFailure::FloodWait(Duration::from_secs(601))
        );
        // A zero-second wait must still keep the retry out of the window.
        assert_eq!(
            classify_rpc_error(420, "FLOOD_WAIT", Some(0)),
            RpcFailure::FloodWait(Duration::from_secs(1))
        );
    }

    #[test]
    fn untimed_rate_limit_variants_are_not_retried() {
        assert_eq!(
            classify_rpc_error(420, "FLOOD_WAIT", None),
            RpcFailure::FloodWithoutDuration {
                name: "FLOOD_WAIT".to_owned()
            }
        );
    }

    #[test]
    fn other_rpc_errors_are_fatal() {
        assert_eq!(
            classify_rpc_error(500, "INTERNAL", Some(2)),
            RpcFailure::Fatal
        );
        assert_eq!(
            classify_rpc_error(400, "MESSAGE_ID_INVALID", None),
            RpcFailure::Fatal
        );
    }

    #[test]
    fn attempts_are_allowed_up_to_the_budget() {
        assert_eq!(next_attempt(1, 3), Attempt::Retry);
        assert_eq!(next_attempt(2, 3), Attempt::Retry);
        assert_eq!(next_attempt(3, 3), Attempt::Exhausted { attempts: 3 });
    }

    #[test]
    fn a_single_attempt_budget_never_retries() {
        assert_eq!(next_attempt(1, 1), Attempt::Exhausted { attempts: 1 });
    }

    #[test]
    fn default_limits_bound_the_backend_without_changing_single_object_work() {
        let limits = GrammersLimits::default();

        assert!(limits.max_concurrent_requests >= 1);
        assert!(limits.max_attempts >= 1);
        // A full part/chunk must transfer well inside the default deadline
        // even on a slow link.
        assert!(limits.io_timeout >= Duration::from_secs(30));
    }

    #[test]
    fn sanitized_limits_reject_degenerate_values() {
        let limits = GrammersLimits {
            max_concurrent_requests: 0,
            io_timeout: Duration::ZERO,
            max_attempts: 0,
        }
        .sanitized();

        assert_eq!(limits.max_concurrent_requests, 1);
        assert_eq!(limits.io_timeout, MIN_IO_TIMEOUT);
        assert_eq!(limits.max_attempts, 1);
    }

    #[test]
    fn sanitized_limits_cap_an_unusable_concurrency_bound() {
        let limits = GrammersLimits {
            max_concurrent_requests: Semaphore::MAX_PERMITS + 1,
            ..Default::default()
        }
        .sanitized();

        assert_eq!(limits.max_concurrent_requests, Semaphore::MAX_PERMITS);
    }

    #[test]
    fn sanitized_limits_leaves_usable_values_alone() {
        let limits = GrammersLimits {
            max_concurrent_requests: 4,
            io_timeout: Duration::from_secs(30),
            max_attempts: 5,
        }
        .sanitized();

        assert_eq!(
            limits,
            GrammersLimits {
                max_concurrent_requests: 4,
                io_timeout: Duration::from_secs(30),
                max_attempts: 5,
            }
        );
    }
}
