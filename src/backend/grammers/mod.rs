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
    message::{InputMessage, Message},
    tl::{functions::upload::SaveBigFilePart, types::InputFileBig},
};
use grammers_mtsender::{InvocationError, SenderPool, SenderPoolFatHandle};
use grammers_session::types::PeerRef;
use sea_orm::DatabaseConnection;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt},
    io::StreamReader,
};
use tracing::{instrument, warn};

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};

use super::{Backend, BackendError, BoxedAsyncReader, ReadOutcome, ReclaimOutcome};

mod session;

const PART_SIZE: usize = 512 * 1024;
const MAX_PARTS: i32 = 4000;
// Telegram file chunk size used for downloads (mirrors grammers'
// `client::files::MAX_CHUNK_SIZE`, which is private in 0.10).
const MAX_CHUNK_SIZE: i32 = 512 * 1024;

// Error code Telegram uses for every timed rate-limit variant
// (`FLOOD_WAIT_X`, `FLOOD_PREMIUM_WAIT_X`, ...).
const FLOOD_ERROR_CODE: i32 = 420;

// Smallest deadline accepted from configuration. A zero timeout would abort
// every request the instant it started, which looks like a total outage
// rather than a misconfiguration.
const MIN_IO_TIMEOUT: Duration = Duration::from_secs(1);

/// How far back into the chat history [`Grammers::reclaim_publish`] looks
/// for a publish by its file name.
///
/// # Why iteration, and not search
///
/// Telegram's message search (`client.search_messages` / `search_all_messages`
/// — the `q` of `messages.search`) indexes **message text**, and the publish
/// identifier is never part of that text: [`Grammers::write`] sends it as the
/// attachment's *file name* and sends no caption. Whether Telegram also
/// indexes an attachment's file name into its text search is a server-side
/// indexing decision that cannot be verified from the client API, so it is not
/// relied on here.
///
/// Walking the history can be verified from the vendored grammers 0.10 source
/// instead, which is what this implementation does. Two facts from
/// `/root/.cargo/registry/src/*/grammers-client-0.10.0`:
///
/// * `Media::Document::name()` (`src/media/media.rs`) returns the file name
///   straight off the `filename` document attribute — the very attribute
///   `src/media/mod.rs` sets from `Uploaded::name()` on upload, i.e. the one
///   [`Grammers::write`] passed as `publish_name`;
/// * `Client::iter_messages` (`src/client/messages.rs`) returns newest-first
///   history with a `.limit(n)`.
///
/// # The limitation this buys
///
/// The sweep can only see this many most-recent messages. A message older than
/// the window is indistinguishable from one that was never published, so a
/// publish that has aged out of the window answers
/// [`ReclaimOutcome::NothingPublished`] and its bytes stay in the chat until an
/// operator deletes them by hand. That is a real leak an unbounded sweep would
/// close and a bounded one cannot: walking the whole history per intent is
/// unbounded work at start-up, which the reconciler contract forbids. The
/// window is sized for the gap between a write losing its answer and the next
/// start-up.
const PUBLISH_LOOKBACK_MESSAGES: usize = 500;

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

/// Scope every instrument below is registered under.
///
/// The same string as `SERVICE_NAME` in `main.rs`, which is what this crate's
/// traces are scoped under. The constant cannot literally be shared: `main.rs`
/// is the binary and this is the library, and a metric recorded under a
/// different scope than its trace would not join up in a collector.
const TELEMETRY_SCOPE: &str = "tele-s3";

/// Rate limits this process hit, by Telegram's error name and by our own
/// operation. A count is the number an operator can put next to the
/// concurrency and timeout settings; the split by name is what separates an
/// ordinary `FLOOD_WAIT` from a `FLOOD_PREMIUM_WAIT`, which is a different
/// problem with a different fix.
const FLOOD_LIMIT_COUNTER: &str = "tele_s3.telegram.flood_limits";

/// How long each rate-limit window actually slept, in seconds.
///
/// The distribution, not the count: a single mean tells an operator nothing
/// about whether the retry budget can absorb the waits Telegram is asking for.
const FLOOD_WAIT_HISTOGRAM: &str = "tele_s3.telegram.flood_wait_duration";

/// Outbound Telegram RPCs, by operation and outcome.
const RPC_CALL_COUNTER: &str = "tele_s3.telegram.rpc_calls";

/// Outbound Telegram RPC latency in seconds, by operation.
const RPC_DURATION_HISTOGRAM: &str = "tele_s3.telegram.rpc_duration";

/// How one outbound Telegram RPC ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcOutcome {
    /// The server answered.
    Ok,
    /// The server answered with an error.
    RpcError,
    /// The request never got an answer inside the deadline.
    Timeout,
}

impl RpcOutcome {
    /// The wire value of an outcome attribute: a closed vocabulary, so a
    /// collector can group on it without parsing prose.
    fn as_str(self) -> &'static str {
        match self {
            RpcOutcome::Ok => "ok",
            RpcOutcome::RpcError => "rpc_error",
            RpcOutcome::Timeout => "timeout",
        }
    }
}

/// Attributes every rate-limit hit is recorded under.
///
/// Pure, so the vocabulary an operator queries by is testable without a meter,
/// a reader or a collector behind it.
fn flood_limit_attributes(telegram_error: &str, operation: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("telegram_error", telegram_error.to_owned()),
        KeyValue::new("operation", operation.to_owned()),
    ]
}

/// Attributes every rate-limit wait is recorded under.
///
/// The wait is attributed to the operation that had to sit through it and to
/// the peer it was addressed to — the two facts that make a process-wide
/// guard diagnosable — rather than to whichever request happened to hold the
/// window open, which the guard deliberately does not remember.
fn flood_wait_attributes(operation: &str, peer: &str) -> [KeyValue; 2] {
    [
        KeyValue::new("operation", operation.to_owned()),
        KeyValue::new("peer", peer.to_owned()),
    ]
}

/// Attributes every outbound RPC is recorded under.
fn rpc_attributes(operation: &str, outcome: RpcOutcome) -> [KeyValue; 2] {
    [
        KeyValue::new("operation", operation.to_owned()),
        KeyValue::new("outcome", outcome.as_str()),
    ]
}

/// The instruments this crate records through.
///
/// Built from the process-wide meter, which is a no-op meter until
/// `main.rs` installs a meter provider — and it installs one only when an
/// OTLP endpoint is configured. So the default, self-hosted deployment
/// records nothing, exports nothing and starts no thread, and the calls below
/// cost a no-op dispatch rather than an aggregation.
#[derive(Clone)]
struct TelegramMetrics {
    flood_limits: Counter<u64>,
    flood_wait_duration: Histogram<f64>,
    rpc_calls: Counter<u64>,
    rpc_duration: Histogram<f64>,
}

impl TelegramMetrics {
    /// Build the instruments on `meter`.
    fn from_meter(meter: &Meter) -> Self {
        Self {
            flood_limits: meter
                .u64_counter(FLOOD_LIMIT_COUNTER)
                .with_description("Telegram rate limits hit, by error name and operation")
                .with_unit("{limit}")
                .build(),
            flood_wait_duration: meter
                .f64_histogram(FLOOD_WAIT_HISTOGRAM)
                .with_description("Seconds slept off a Telegram rate-limit window")
                .with_unit("s")
                .build(),
            rpc_calls: meter
                .u64_counter(RPC_CALL_COUNTER)
                .with_description("Outbound Telegram RPCs, by operation and outcome")
                .with_unit("{call}")
                .build(),
            rpc_duration: meter
                .f64_histogram(RPC_DURATION_HISTOGRAM)
                .with_description("Outbound Telegram RPC latency in seconds")
                .with_unit("s")
                .build(),
        }
    }

    /// The process-wide instruments.
    fn global() -> Self {
        Self::from_meter(&opentelemetry::global::meter(TELEMETRY_SCOPE))
    }

    /// One rate limit was hit, whatever it turned into afterwards.
    fn record_flood(&self, telegram_error: &str, operation: &str) {
        self.flood_limits
            .add(1, &flood_limit_attributes(telegram_error, operation));
    }

    /// One rate-limit window was slept off in full.
    fn record_flood_wait(&self, operation: &str, peer: &str, elapsed: Duration) {
        self.flood_wait_duration.record(
            elapsed.as_secs_f64(),
            &flood_wait_attributes(operation, peer),
        );
    }

    /// One outbound Telegram RPC ended.
    fn record_rpc(&self, operation: &str, outcome: RpcOutcome, elapsed: Duration) {
        self.rpc_calls.add(1, &rpc_attributes(operation, outcome));
        self.rpc_duration
            .record(elapsed.as_secs_f64(), &rpc_attributes(operation, outcome));
    }
}

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
    /// Where the MTProto session (auth keys and DC state) is persisted.
    ///
    /// Not the object metadata store, although `main` passes it the same
    /// connection. The tables are created on first use, and losing this
    /// database costs a re-login rather than data: the bytes are messages in
    /// the peer, and only the keys naming them live in the metadata store.
    pub db: DatabaseConnection,
    /// The chat every object is stored in, resolved once at
    /// [`Grammers::init`].
    ///
    /// A key is a message id in *this* chat, so an instance built for a
    /// different `username` cannot read objects written by another — the
    /// message ids simply do not name the same messages.
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
    metrics: TelegramMetrics,
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

/// How a declared `size` is cut into Telegram parts.
///
/// The whole of the size accounting [`Grammers::write`] performs, with no
/// reader and no client in it, so both sides of the "exactly `size` bytes"
/// contract and the arithmetic at a [`PART_SIZE`] boundary are testable
/// without a Telegram session.
#[derive(Debug, PartialEq, Eq)]
struct BodyPlan {
    /// `file_total_parts` for the upload, and `parts` for the finished one.
    total_parts: i32,
    /// The declared length, kept so the over-long error can name the number
    /// the body was measured against.
    declared: u64,
    /// The length of each part, in upload order. Empty for a zero-byte body,
    /// which is uploaded as no parts at all.
    lengths: Vec<usize>,
}

impl BodyPlan {
    /// Cut `size` bytes into full parts plus at most one short trailing part.
    ///
    /// A `size` that is an exact multiple of [`PART_SIZE`] gets no short
    /// part; one byte more gets a one-byte one. A zero `size` gets neither.
    fn for_size(size: usize) -> Self {
        let mut lengths = Vec::with_capacity(size.div_ceil(PART_SIZE));
        let mut remaining = size;

        while remaining > 0 {
            let len = PART_SIZE.min(remaining);
            remaining -= len;
            lengths.push(len);
        }

        Self {
            total_parts: lengths.len() as i32,
            declared: size as u64,
            lengths,
        }
    }
}

/// The reader carried on past the `size` the write declared.
///
/// Deliberately *not* [`BackendError::ExceedLimitSize`]: that variant means
/// "this object will not fit, send less", and it is the only one whose
/// message reaches the client. The object the caller asked for fits — what
/// disagrees is the body with its own declared length, and only a single byte
/// past the end is ever read, so there is no honest `actual` to report and no
/// advice that would help the caller.
///
/// It is also not claimed to be a client error. `write` cannot tell a client
/// that overran its `Content-Length` from a caller above it that passed the
/// wrong `size`, and attributing the fault would be a guess. So it is an
/// ordinary backend failure and lands on `InternalError` through the existing
/// catch-all mapping, which keeps the reason as the error's `source` for the
/// logs. Logged here as well, because the response deliberately does not
/// carry it and this is the one place both numbers are in hand.
fn overlong_body(size: u64) -> BackendError {
    tracing::error!(
        declared_bytes = size,
        error.type = "write_body_overlong",
        "backend write fault: the body is longer than the declared size"
    );

    BackendError::Other(
        std::io::Error::other(format!(
            "write body is longer than the declared {size} bytes"
        ))
        .into(),
    )
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

/// Whether the publish step of [`Grammers::write`] left a message behind.
///
/// The upload is already complete when the message is published, so the only
/// question this answers is whether the chat may now be holding a message
/// that nothing else in the system knows about.
#[derive(Debug, PartialEq, Eq)]
enum PublishOutcome {
    /// Nothing was published: the request never reached Telegram, or
    /// Telegram answered it with an error. The bytes are unreferenced part
    /// state that the server reclaims on its own.
    NothingPublished,
    /// A message may exist in the peer chat holding the uploaded bytes,
    /// with no key and therefore no metadata row naming it.
    PossiblyPublished,
}

/// How the publish step of [`Grammers::write`] failed, without any client type
/// in it.
///
/// Deliberately separate from [`AttemptError`]: the classification below must
/// be exercisable with no session, no connection and no Telegram server, and
/// only the conversion needs to know what an [`InvocationError`] is.
#[derive(Debug, PartialEq, Eq)]
enum PublishFailure {
    /// Telegram returned an error for this request.
    Rejected { rpc_error: String },
    /// The request outlived the per-request deadline after it was dispatched.
    TimedOut { after: Duration },
    /// The request failed below the RPC layer once it was on the wire, so the
    /// server may already have created the message. `kind` names the layer
    /// for the log.
    ResponseLost { kind: &'static str },
}

/// Which publish failures may have created a message.
///
/// The rule is deliberately one-sided: only a rejection Telegram reported for
/// *this* request is treated as proof that nothing was published. A deadline
/// and every failure below the RPC layer leave the outcome unknown, so they
/// are classified [`PublishOutcome::PossiblyPublished`] and the operator is
/// handed the identifiers to look for.
///
/// What is coarse about it:
///
/// * `InvocationError::Session` usually means the request was never
///   dispatched — the session was consulted before the peer was known — so
///   classifying it as possibly published is the conservative over-approximation
///   rather than a claim that a message exists.
/// * The reverse coarseness sits on the other arm: Telegram's own error
///   contract does not forbid a server-side error *after* the side effect,
///   so [`PublishOutcome::NothingPublished`] really means "Telegram
///   reported no error for this request", not "no message exists".
///
/// Either way no retry can repair it: MTProto offers no idempotency key for
/// message creation, so re-sending an ambiguous `send_message` risks
/// publishing the object twice. See [`Grammers::write`].
fn classify_publish_failure(failure: &PublishFailure) -> PublishOutcome {
    match failure {
        PublishFailure::Rejected { .. } => PublishOutcome::NothingPublished,
        PublishFailure::TimedOut { .. } | PublishFailure::ResponseLost { .. } => {
            PublishOutcome::PossiblyPublished
        }
    }
}

impl PublishFailure {
    /// Name the layer that lost the response, without guessing where it was
    /// lost.
    fn from_attempt(err: &AttemptError) -> Self {
        match err {
            AttemptError::Timeout(after) => PublishFailure::TimedOut { after: *after },
            AttemptError::Rpc(err) => match err.as_ref() {
                InvocationError::Rpc(rpc) => PublishFailure::Rejected {
                    rpc_error: match rpc.value {
                        Some(value) => format!("{} ({value})", rpc.name),
                        None => rpc.name.clone(),
                    },
                },
                InvocationError::Io(_) => PublishFailure::ResponseLost { kind: "io" },
                InvocationError::Deserialize(_) => PublishFailure::ResponseLost {
                    kind: "deserialize",
                },
                InvocationError::Transport(_) => PublishFailure::ResponseLost { kind: "transport" },
                // `Session`, `Dropped`, `InvalidDc`, `Authentication` and any
                // variant a later grammers adds: none of them is a statement
                // from the server that it refused the create, so none is
                // allowed to claim the upload published nothing.
                _ => PublishFailure::ResponseLost {
                    kind: "uncategorised",
                },
            },
        }
    }

    /// The error the caller sees. No key is returned on any of these paths.
    fn into_backend_error(self) -> BackendError {
        match self {
            PublishFailure::Rejected { rpc_error } => BackendError::Other(rpc_error.into()),
            PublishFailure::TimedOut { after } => BackendError::Other(
                format!("Telegram publish request exceeded the {after:?} deadline").into(),
            ),
            PublishFailure::ResponseLost { kind } => {
                BackendError::Other(format!("Telegram publish response lost ({kind})").into())
            }
        }
    }
}

/// Why [`Grammers::write`] published no message and can name no key.
enum PublishFault {
    /// The request was never dispatched — a local fault, here the flood
    /// guard's mutex. Nothing reached Telegram, so nothing was published.
    NotDispatched(BackendError),
    /// The request was dispatched and failed. Whether a message exists
    /// depends on the failure; see [`classify_publish_failure`].
    Dispatched(PublishFailure),
}

/// Turn a publish fault into the caller's error, logging the only record of
/// a possible orphan.
///
/// `file_id` and `name` are the upload's coordinates: on the ambiguous
/// outcomes a message holding those bytes may be sitting in the peer chat
/// with no key pointing at it, so no metadata row exists and
/// `reconcile_orphan_blobs` — which drops rows, not backend objects — can
/// never see it. They are written here and nowhere else, which makes this log
/// line the operator's only way to find and delete that message by hand.
fn unpublished_upload(fault: PublishFault, file_id: i64, name: &str) -> BackendError {
    match fault {
        PublishFault::NotDispatched(err) => err,
        PublishFault::Dispatched(failure) => {
            if classify_publish_failure(&failure) == PublishOutcome::PossiblyPublished {
                tracing::error!(
                    file_id,
                    name,
                    outcome = "possibly-published",
                    error.type = "unpublished_upload",
                    "upload may exist as a Telegram message but no key was returned; \
                     it is unreachable by key and must be deleted from the chat by hand"
                );
            }

            failure.into_backend_error()
        }
    }
}

/// Why a [`Grammers`] key could not be turned into a message to fetch.
///
/// Split out of [`Grammers::read`] and given its own mapping into
/// [`ReadOutcome`] so the decision is testable without a live Telegram
/// session: the two facts here are all the classification depends on, and
/// only obtaining them needs a client.
#[derive(Debug, PartialEq, Eq)]
enum KeyFault {
    /// The key is not a message id, so it is not one this backend minted.
    /// A key from another backend lands here too.
    Unaddressable,
    /// The message exists but carries no media, so there is nothing to
    /// stream. The object is present and unreadable, not absent.
    NoMedia,
}

impl From<KeyFault> for ReadOutcome {
    fn from(fault: KeyFault) -> Self {
        match fault {
            KeyFault::Unaddressable => ReadOutcome::Fault(BackendError::InvariantViolation {
                detail: "key is not a message id this backend minted",
            }),
            KeyFault::NoMedia => ReadOutcome::Fault(BackendError::InvariantViolation {
                detail: "the message a key names exists but carries no media",
            }),
        }
    }
}

/// The event a Telegram rate limit produces when it asked for a wait.
///
/// `telegram_error` is Telegram's own name for the failure — `FLOOD_WAIT_5`,
/// `FLOOD_PREMIUM_WAIT_X` — and it is deliberately *not* `operation`:
/// `operation` names which of ours was in flight, and overloading the two
/// makes the query "was this a FLOOD_PREMIUM, or a write that got blocked?"
/// unanswerable.
///
/// Pure, so the event's shape is testable without a `Grammers`, a client or a
/// Telegram session behind it.
fn log_flood_hit(operation: &str, peer: &str, telegram_error: &str, code: i32, wait: Duration) {
    tracing::warn!(
        operation,
        telegram_error,
        rpc_error_code = code,
        peer,
        duration_ms = wait.as_millis() as u64,
        "hit a Telegram rate limit; blocking every outbound request for that window"
    );
}

/// The event a Telegram rate limit produces when it named no wait at all.
///
/// The anomalous case: the server refused the request and would not say for
/// how long, so there is nothing to sleep off and nothing to retry into. It
/// used to produce no event at all, which left the operator with only the
/// genericised `BackendError::Other` a caller sees.
fn log_flood_without_duration(operation: &str, peer: &str, telegram_error: &str, code: i32) {
    tracing::warn!(
        operation,
        telegram_error,
        rpc_error_code = code,
        peer,
        error.type = "flood_without_duration",
        "hit a Telegram rate limit that named no wait window; the request cannot be retried"
    );
}

/// The event a flood-guard wait produces, inside [`Grammers::sleep_off_flood`]'s
/// span.
fn log_flood_wait(operation: &str, peer: &str, waited: Duration) {
    tracing::warn!(
        operation,
        peer,
        duration_ms = waited.as_millis() as u64,
        "flood guard active; sleeping for the remaining rate-limit window"
    );
}

/// The flood guard's mutex is poisoned, so its bookkeeping is of unknown
/// consistency for the life of the process.
///
/// The `detail`/`lock` a permanent fault carries is deliberately kept out of
/// the response the client sees, which leaves the log as the only place it
/// is ever written down — so it is written here, where the lock is named.
fn poisoned_flood_guard() -> BackendError {
    tracing::error!(
        lock = "flood_guard",
        error.type = "poisoned_lock",
        "flood guard mutex poisoned; rate-limit waits are no longer tracked"
    );
    BackendError::PoisonedLock {
        lock: "flood_guard",
    }
}

/// Resolve a key to the message id to fetch.
///
/// `Err` carries the [`ReadOutcome`] to report. A key this backend cannot
/// address is [`ReadOutcome::Fault`] and never [`ReadOutcome::Missing`]:
/// saying the object is absent would tell the operator it was deleted when
/// in fact it was never one of ours to delete.
///
/// Pure, so the malformed-key decision is testable without a client.
fn resolve_key(key: &str) -> Result<i32, ReadOutcome> {
    key.parse::<i32>()
        .map_err(|_| KeyFault::Unaddressable.into())
}

/// The message id a `delete` should target, or `None` when the key names
/// nothing this backend wrote.
///
/// Deliberately the opposite of [`resolve_key`]: where `read` calls an
/// unparseable key a fault, `delete` calls it absent, because the idempotency
/// contract makes deleting something that is not there a success. Pure, so
/// that half of the contract is testable without a client.
fn delete_target(key: &str) -> Option<i32> {
    key.parse::<i32>().ok()
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

/// Name a caller-supplied publish identifier publishes under.
///
/// The published file name is the caller's identifier and nothing else: the
/// caller recorded it as a publish intent before this write, so a publish
/// whose answer was lost is still findable by this exact string. Pure, so
/// the "the name is the identifier" contract is testable without a session.
fn publish_name(publish_id: &str) -> &str {
    publish_id
}

/// Whether an attachment's file name is the publish being looked for.
///
/// The name is the *only* coordinate a lost publish can be found by, so the
/// comparison is exact and whole: no prefix match (a partial match would
/// happily delete the wrong message), no case folding (Telegram preserves the
/// name, so the caller must match what it sent), and no trimming.
fn attachment_is_publish(file_name: Option<&str>, publish_id: &str) -> bool {
    file_name == Some(publish_id)
}

impl Grammers {
    /// Upload the parts `plan` describes from `reader`, then prove the reader
    /// really ended.
    ///
    /// Split out of [`Grammers::write`] with the client reached only through
    /// `upload_part`, so the size accounting and both directions of the
    /// "exactly `size` bytes" contract are exercisable with a fake and no
    /// Telegram session.
    ///
    /// The declared length is enforced on both sides. Short: a part is filled
    /// with `read_exact`, so a body that ends early fails here. Long: once
    /// the last planned part is in, one more byte is read — a body that still
    /// has one is over-long and fails.
    ///
    /// That order is the whole point. The over-long check is only sound
    /// *after* the parts are up, because a stream that has not been read past
    /// its declared end cannot be shown to have ended there. In exchange the
    /// check still runs before the message is sent, so nothing is published:
    /// what is left behind is Telegram-side part state under a `file_id` no
    /// message references, which the server reclaims on its own. No key is
    /// ever returned for it, so the reconciler — which drops metadata *rows*,
    /// not backend objects — has nothing to see and nothing to miss. The
    /// residual window is that orphaned part state, and it holds no bytes the
    /// caller can reach.
    async fn upload_parts<R, F, Fut>(
        reader: &mut R,
        plan: &BodyPlan,
        mut upload_part: F,
    ) -> Result<(), BackendError>
    where
        R: AsyncRead + Unpin,
        F: FnMut(i32, Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), BackendError>>,
    {
        for (part_index, len) in plan.lengths.iter().copied().enumerate() {
            let mut buffer = vec![0u8; len];

            // `read_exact`, not `read`: a body that ends before `size` must
            // fail here rather than publish a part padded with zeroes.
            reader
                .read_exact(&mut buffer)
                .await
                .map_err(|e| BackendError::Other(e.into()))?;

            upload_part(part_index as i32, buffer).await?;

            tracing::debug!(
                part_index = part_index as u32,
                bytes = len,
                "uploaded part to Telegram"
            );
        }

        // The declared `size` is now uploaded. One byte more is all it takes
        // to tell a body that ended at `size` from one that did not, and a
        // byte here means the trailing bytes would otherwise be dropped with
        // no error and a key returned for a short object.
        let mut trailing = [0u8; 1];
        let overlong = reader
            .read(&mut trailing)
            .await
            .map_err(|e| BackendError::Other(e.into()))?;

        if overlong != 0 {
            return Err(overlong_body(plan.declared));
        }

        Ok(())
    }

    #[instrument(skip(config), level = "debug", err)]
    pub async fn init(config: GrammersConfig) -> anyhow::Result<Self> {
        let limits = config.limits.sanitized();
        tracing::info!(
            max_concurrent_requests = limits.max_concurrent_requests,
            io_timeout_ms = limits.io_timeout.as_millis() as u64,
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
            metrics: TelegramMetrics::global(),
        })
    }

    /// The chat every request this backend makes is addressed to, as
    /// Telegram's own dialog id.
    ///
    /// The flood guard is one process-wide mutex, so a rate limit one request
    /// draws holds up every *other* request too. That is what makes the peer
    /// worth naming on a flood: an operator reading one chat's rate limit has
    /// to be able to tell which chat drew it.
    fn peer_name(&self) -> String {
        self.peer.id.to_string()
    }

    #[instrument(skip(self), level = "debug")]
    pub fn close(self) {
        self.sender_pool_handle.quit();
    }

    /// Sleep off whatever the flood guard is holding, and say so in its own
    /// span.
    ///
    /// The span is the point: a wait is a thing that happened, with a
    /// duration, and an operator tuning the concurrency and timeout knobs
    /// needs to find one wait and read it rather than infer it from an event
    /// riding whichever request span happened to be current.
    #[instrument(
        name = "telegram.flood_wait",
        skip(self, peer),
        fields(operation = operation, peer = %peer, duration_ms = duration.as_millis() as u64)
    )]
    async fn sleep_off_flood(&self, operation: &str, peer: &str, duration: Duration) {
        self.metrics.record_flood_wait(operation, peer, duration);
        log_flood_wait(operation, peer, duration);
        tokio::time::sleep(duration).await;
    }

    /// Sleep off the remaining rate-limit window, if one is still in force.
    ///
    /// `operation` is the operation about to go out — the one that has to sit
    /// through whatever window another request opened.
    async fn check_flood_wait(&self, operation: &str) -> Result<(), BackendError> {
        let wait_time = {
            let guard = self
                .flood_guard
                .lock()
                .map_err(|_| poisoned_flood_guard())?;

            if let Some(until) = *guard {
                let now = Instant::now();
                if until > now { Some(until - now) } else { None }
            } else {
                None
            }
        };

        if let Some(duration) = wait_time {
            self.sleep_off_flood(operation, &self.peer_name(), duration)
                .await;

            let mut guard = self
                .flood_guard
                .lock()
                .map_err(|_| poisoned_flood_guard())?;
            *guard = None;
        }

        Ok(())
    }

    /// Turn a server error into a retry decision, recording whatever it was.
    ///
    /// `operation` is the operation that was in flight when the server
    /// answered, which is the fact that makes the process-wide guard
    /// diagnosable: the same 420 means something quite different depending on
    /// which request drew it.
    async fn catch_flood_error(
        &self,
        err: &InvocationError,
        operation: &str,
    ) -> Result<bool, BackendError> {
        let InvocationError::Rpc(rpc_err) = err else {
            return Ok(false);
        };

        let peer = self.peer_name();

        match classify_rpc_error(rpc_err.code, &rpc_err.name, rpc_err.value) {
            RpcFailure::FloodWait(duration) => {
                {
                    let mut guard = self
                        .flood_guard
                        .lock()
                        .map_err(|_| poisoned_flood_guard())?;
                    *guard = Some(Instant::now() + duration);
                }

                self.metrics.record_flood(&rpc_err.name, operation);
                log_flood_hit(operation, &peer, &rpc_err.name, rpc_err.code, duration);

                Ok(true)
            }
            RpcFailure::FloodWithoutDuration { name } => {
                // A rate limit the server would not put a duration on. This is
                // the anomalous case and the one most worth seeing, and it used
                // to produce no event at all: it became a `BackendError::Other`
                // whose wording is genericised away before a client ever sees
                // it. The request still fails exactly as it did.
                self.metrics.record_flood(&name, operation);
                log_flood_without_duration(operation, &peer, &name, rpc_err.code);

                Err(BackendError::Other(
                    format!("{name} without duration").into(),
                ))
            }
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
    async fn attempt<T, F>(&self, operation: &str, rpc: F) -> Result<T, AttemptError>
    where
        F: Future<Output = Result<T, InvocationError>>,
    {
        let _permit: OwnedSemaphorePermit = self
            .inflight
            .clone()
            .acquire_owned()
            .await
            .expect("the concurrency limiter is never closed");

        let started = Instant::now();

        match tokio::time::timeout(self.limits.io_timeout, rpc).await {
            Ok(Ok(value)) => {
                self.metrics
                    .record_rpc(operation, RpcOutcome::Ok, started.elapsed());
                Ok(value)
            }
            Ok(Err(err)) => {
                self.metrics
                    .record_rpc(operation, RpcOutcome::RpcError, started.elapsed());
                Err(AttemptError::Rpc(Box::new(err)))
            }
            Err(_elapsed) => {
                self.metrics
                    .record_rpc(operation, RpcOutcome::Timeout, started.elapsed());
                Err(AttemptError::Timeout(self.limits.io_timeout))
            }
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
            self.check_flood_wait("download_chunk").await?;
            attempt += 1;

            let mut download_iter = self
                .client
                .iter_download(media)
                .chunk_size(MAX_CHUNK_SIZE)
                .skip_chunks(skip_chunks as i32);

            let outcome = self.attempt("download_chunk", download_iter.next()).await;

            match outcome {
                Ok(Some(chunk)) => return Ok(Some(chunk)),
                Ok(None) => return Ok(None),
                Err(AttemptError::Rpc(err)) => {
                    if !self.catch_flood_error(&err, "download_chunk").await? {
                        return Err(AttemptError::Rpc(err).into_backend_error());
                    }

                    match next_attempt(attempt, self.limits.max_attempts) {
                        Attempt::Retry => {}
                        Attempt::Exhausted { attempts } => {
                            warn!(
                                attempts,
                                operation = "download_chunk",
                                error.type = "rpc",
                                error.message = %err,
                                "download retry budget exhausted"
                            );
                            return Err(AttemptError::Rpc(err).into_backend_error());
                        }
                    }
                }
                Err(AttemptError::Timeout(timeout)) => {
                    warn!(
                        operation = "download_chunk",
                        timeout_ms = timeout.as_millis() as u64,
                        error.type = "timeout",
                        "download chunk exceeded its deadline"
                    );
                    return Err(AttemptError::Timeout(timeout).into_backend_error());
                }
            }
        }
    }

    /// Id of a recent message in the peer chat whose attachment is named
    /// exactly `publish_id`.
    ///
    /// One bounded walk of [`PUBLISH_LOOKBACK_MESSAGES`] newest messages, each
    /// step under the same concurrency bound and the same per-request deadline
    /// as every other outbound request here (`attempt` is what
    /// [`Grammers::retry_rpc`] uses underneath, and the flood wait is checked
    /// up front exactly as it does).
    ///
    /// The first match wins. A duplicate name would mean two messages holding
    /// the same bytes under one identifier, which the S3 layer cannot produce:
    /// `publish_id` is minted from a UUID and the intent row is its primary
    /// key. So there is nothing a caller could do with a second anyway.
    ///
    /// # The window
    ///
    /// Only the newest [`PUBLISH_LOOKBACK_MESSAGES`] messages are examined.
    /// A publish older than that is reported as not found — see
    /// [`PUBLISH_LOOKBACK_MESSAGES`] for why that is a deliberate limit and
    /// not an oversight.
    async fn find_published_message(&self, publish_id: &str) -> Result<Option<i32>, BackendError> {
        let mut messages = self
            .client
            .iter_messages(self.peer)
            .limit(PUBLISH_LOOKBACK_MESSAGES);

        loop {
            // Not `retry_rpc`: that takes a `FnMut` closure, and a future
            // borrowing `messages` cannot escape one. The bound is applied
            // through `attempt` instead, which is the same request path.
            self.check_flood_wait("iter_messages").await?;

            let message = match self.attempt("iter_messages", messages.next()).await {
                Ok(Some(message)) => message,
                Ok(None) => return Ok(None),
                Err(AttemptError::Timeout(timeout)) => {
                    warn!(
                        operation = "iter_messages",
                        timeout_ms = timeout.as_millis() as u64,
                        error.type = "timeout",
                        "Telegram history request exceeded its deadline"
                    );
                    return Err(AttemptError::Timeout(timeout).into_backend_error());
                }
                // A history request that fails is *not* proof that no message
                // is there: answering "nothing published" off a failed lookup
                // would clear an intent for bytes that may well be sitting in
                // the chat. It fails the call instead.
                Err(AttemptError::Rpc(err)) => {
                    return Err(AttemptError::Rpc(err).into_backend_error());
                }
            };

            // Only an attachment can carry a published name; a photo, a
            // sticker or a text message never can.
            let Some(Media::Document(document)) = message.media() else {
                continue;
            };

            if attachment_is_publish(document.name(), publish_id) {
                return Ok(Some(message.id()));
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
    async fn retry_rpc<T, F, Fut>(
        &self,
        operation: &'static str,
        mut rpc: F,
    ) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, InvocationError>>,
    {
        let mut attempt: u32 = 0;

        loop {
            self.check_flood_wait(operation).await?;
            attempt += 1;

            let err = match self.attempt(operation, rpc()).await {
                Ok(value) => return Ok(value),
                Err(AttemptError::Timeout(timeout)) => {
                    warn!(
                        operation,
                        timeout_ms = timeout.as_millis() as u64,
                        error.type = "timeout",
                        "Telegram request exceeded its deadline"
                    );
                    return Err(AttemptError::Timeout(timeout).into_backend_error());
                }
                Err(AttemptError::Rpc(err)) => err,
            };

            if !self.catch_flood_error(&err, operation).await? {
                return Err(AttemptError::Rpc(err).into_backend_error());
            }

            tracing::debug!(
                operation,
                attempts = attempt,
                error.type = "rpc",
                error.message = %err,
                "retrying Telegram request after a rate limit"
            );

            match next_attempt(attempt, self.limits.max_attempts) {
                Attempt::Retry => {}
                Attempt::Exhausted { attempts } => {
                    warn!(
                        operation,
                        attempts,
                        error.type = "rpc",
                        error.message = %err,
                        "Telegram retry budget exhausted"
                    );
                    return Err(AttemptError::Rpc(err).into_backend_error());
                }
            }
        }
    }

    /// Publish the uploaded parts as a message in the peer chat.
    ///
    /// The flood-wait loop is [`Grammers::retry_rpc`]'s: a rate limit is
    /// slept off and the create is repeated, which is safe because nothing
    /// has been created yet. Everything else stops the loop, and the failure
    /// is reported as a [`PublishFault`] rather than flattened into a
    /// [`BackendError`], because on the ambiguous outcomes the caller must
    /// learn that a message may exist behind the error.
    ///
    /// Split out of [`Grammers::write`] so the classification in
    /// [`classify_publish_failure`] is testable with no client in it.
    async fn publish_message(&self, draft: InputMessage) -> Result<Message, PublishFault> {
        let mut attempt: u32 = 0;

        loop {
            // Local fault: nothing has been dispatched, so nothing can have
            // been published.
            self.check_flood_wait("publish_message")
                .await
                .map_err(PublishFault::NotDispatched)?;

            attempt += 1;

            let err = match self
                .attempt(
                    "publish_message",
                    self.client.send_message(self.peer, draft.clone()),
                )
                .await
            {
                Ok(message) => return Ok(message),
                Err(err) => err,
            };

            let failure = PublishFailure::from_attempt(&err);

            // Only a rate limit is retried. A deadline is not: the message
            // may already have been created and MTProto has no idempotency key
            // for message creation, so re-sending would publish a duplicate.
            let retryable = match &err {
                AttemptError::Rpc(err) => {
                    match self.catch_flood_error(err, "publish_message").await {
                        Ok(retry) => retry,
                        // A flood wait with no duration is still a rejection of
                        // this request, so the failure keeps its own wording.
                        Err(_) => return Err(PublishFault::Dispatched(failure)),
                    }
                }
                AttemptError::Timeout(timeout) => {
                    warn!(
                        operation = "publish_message",
                        timeout_ms = timeout.as_millis() as u64,
                        error.type = "timeout",
                        "Telegram publish exceeded its deadline"
                    );
                    return Err(PublishFault::Dispatched(failure));
                }
            };

            if !retryable {
                return Err(PublishFault::Dispatched(failure));
            }

            tracing::debug!(
                operation = "publish_message",
                attempts = attempt,
                error.type = "rpc",
                error.message = ?failure,
                "retrying Telegram publish after a rate limit"
            );

            match next_attempt(attempt, self.limits.max_attempts) {
                Attempt::Retry => {}
                Attempt::Exhausted { attempts } => {
                    warn!(
                        attempts,
                        operation = "publish_message",
                        error.type = "rpc",
                        error.message = ?failure,
                        "Telegram publish retry budget exhausted"
                    );
                    return Err(PublishFault::Dispatched(failure));
                }
            }
        }
    }
}

#[async_trait]
impl Backend for Grammers {
    /// Store `size` bytes as a message in the peer chat and return its id.
    ///
    /// The message is published under `publish_id`, the identifier the caller
    /// chose and recorded as a publish intent before calling. It is the only
    /// thing tying an orphaned publish back to this layer, so it must reach
    /// the wire unchanged.
    ///
    /// # The residual window this cannot close
    ///
    /// The upload and the publish are two requests, and only the first is
    /// idempotent. Once every part is in, `send_message` turns the uploaded
    /// bytes into a real message — and it is deliberately not retried on a
    /// deadline, because MTProto offers no idempotency key for message
    /// creation: a retry after a lost answer risks publishing the object
    /// twice.
    ///
    /// So if the create **succeeds server-side but its answer is lost**, or
    /// the attempt budget is spent after Telegram has already created the
    /// message, `write` fails and returns no key. The chat then holds a
    /// message that nothing in the system points at: no key means no metadata
    /// row, and `reconcile_orphan_blobs` removes metadata *rows*, not backend
    /// objects, so it has nothing to see and nothing to miss. That leak is
    /// **not closed** and cannot be closed by retrying.
    ///
    /// What this function does instead is make it findable.
    /// [`classify_publish_failure`] separates the outcomes where the answer
    /// proves nothing was published from the ones where a message may exist,
    /// and on the ambiguous side [`unpublished_upload`] logs the upload's
    /// `file_id` and `name` — after this failure those identifiers exist
    /// nowhere else, so the log is the operator's only way to locate the
    /// orphan and delete it from the chat by hand.
    ///
    /// A follow-up scope owns the real repair: grammers 0.10 does expose
    /// `search_message`/`search_all_message`, which could find a message that
    /// was created without a key and mint one for it. That needs a durable
    /// record of the intended publish plus a backend-aware reconciler, and is
    /// deliberately out of scope here.
    #[instrument(skip(self, reader), level = "debug", ret, err)]
    async fn write(
        &self,
        publish_id: String,
        size: u64,
        reader: BoxedAsyncReader,
    ) -> Result<String, BackendError> {
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

        // The published file name is the caller's identifier verbatim. It is
        // deliberately opaque and carries no bucket or key: it becomes
        // visible in the peer chat, and it is the one coordinate a later
        // reconciler can search a lost publish by.
        let name = publish_name(&publish_id).to_owned();
        let mut compat_reader = reader.compat();

        self.check_flood_wait("write").await?;

        let plan = BodyPlan::for_size(size as usize);
        let total_parts = plan.total_parts;

        Self::upload_parts(&mut compat_reader, &plan, |file_part, bytes| {
            let bytes = bytes.clone();

            async move {
                // Retry the part through flood waits; re-uploading the same
                // part index is idempotent on Telegram's side. Each attempt
                // carries the same `file_id`/`file_part` and the same bytes.
                self.retry_rpc("save_big_file_part", || async {
                    self.client
                        .invoke(&SaveBigFilePart {
                            file_id,
                            file_part,
                            file_total_parts: total_parts,
                            // cloned so a failed attempt can be retried with
                            // the same bytes
                            bytes: bytes.clone(),
                        })
                        .await
                })
                .await
                .map(|_| ())
            }
        })
        .await?;

        let uploaded = Uploaded::from_raw(
            InputFileBig {
                id: file_id,
                parts: total_parts,
                name: name.clone(),
            }
            .into(),
        );

        let draft_message = InputMessage::new().file(uploaded).silent(true);

        // The parts are already uploaded, so this is a single metadata RPC
        // against the peer. It is deliberately *not* retried on a deadline:
        // the message may still have been created, and sending it again
        // would publish a duplicate. A failure here reports no key, so the
        // outcome is classified and, where a message may exist, logged with
        // the identifiers that name the upload — see the doc comment above.
        let message = self
            .publish_message(draft_message)
            .await
            .map_err(|fault| unpublished_upload(fault, file_id, &name))?;
        let message_id = message.id().to_string();

        Ok(message_id)
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<ReadOutcome, BackendError> {
        // A key this backend cannot address is a fault, not an absence:
        // saying it is missing would tell the operator the object was
        // deleted when it was never one of ours. The fault's `detail` never
        // reaches the client, so it is logged here, where the blob id is
        // still in hand.
        let message_id = match resolve_key(&key) {
            Ok(message_id) => message_id,
            Err(fault) => {
                tracing::error!(
                    key = %key,
                    error.type = "key_not_minted",
                    "backend read fault: key is not a message id this backend minted"
                );
                return Ok(fault);
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
            .retry_rpc("get_messages_by_id", || async {
                self.client
                    .get_messages_by_id(self.peer, &[message_id])
                    .await
            })
            .await?;
        let media = {
            let mut messages = media;
            // The peer has no message with this id: the object this key
            // named is genuinely gone. The only case `read` answers
            // `Missing`.
            let message = match messages.pop().flatten() {
                Some(v) => v,
                None => return Ok(ReadOutcome::Missing),
            };

            // The message is there but holds nothing to stream: the object
            // is unreadable, not absent.
            match message.media() {
                Some(v) => v,
                None => {
                    tracing::error!(
                        key = %key,
                        error.type = "no_media",
                        "backend read fault: the message a key names exists but carries no media"
                    );
                    return Ok(KeyFault::NoMedia.into());
                }
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

        Ok(ReadOutcome::Present(reader))
    }

    #[instrument(skip(self), level = "debug", err)]
    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let message_id = match delete_target(&key) {
            Some(message_id) => message_id,
            // Nothing this backend wrote can be named by a key it cannot
            // parse, so there is nothing to delete and the idempotency
            // contract makes that a success.
            None => return Ok(()),
        };

        // Deleting a message that is already gone succeeds server-side, so
        // every attempt carries the same request and the loop stays bounded
        // by the attempt budget.
        self.retry_rpc("delete_messages", || async {
            self.client
                .delete_messages(self.peer, &[message_id])
                .await
                .map(|_deleted| ())
        })
        .await
    }

    /// Walk the recent history of the peer chat looking for a message whose
    /// attachment is named exactly `publish_id`, and delete it.
    ///
    /// The sweep is one bounded pass: at most
    /// [`PUBLISH_LOOKBACK_MESSAGES`] messages are examined, so the cost of a
    /// call is fixed rather than a function of how much traffic the chat has
    /// seen.
    ///
    /// Deletion reuses [`Grammers::delete`], so it carries the same retry loop
    /// and the same idempotency: a message that turns out to have been
    /// removed already is a successful reclaim, not a failure.
    ///
    /// A history request that fails is an `Err`, *not* a
    /// [`ReclaimOutcome::ReclaimFailed`]: the first is "this call could not
    /// find out", the second is "something was found and could not be
    /// removed". Both leave the caller's intent pending, but only the second
    /// claims to know a message is still there.
    async fn reclaim_publish(&self, publish_id: String) -> Result<ReclaimOutcome, BackendError> {
        let message_id = self.find_published_message(&publish_id).await?;

        let Some(message_id) = message_id else {
            return Ok(ReclaimOutcome::NothingPublished);
        };

        match self.delete(message_id.to_string()).await {
            Ok(()) => Ok(ReclaimOutcome::Reclaimed),
            Err(err) => {
                // Logged here rather than answered as `Err`: the caller keeps
                // the intent pending either way, but only this arm says the
                // message is still sitting in the chat, and that is the fact
                // an operator needs.
                tracing::warn!(
                    message_id,
                    publish_id = %publish_id,
                    error.type = "reclaim_publish",
                    error.message = ?err,
                    "found an unconfirmed publish but could not delete it; \
                     the intent stays pending for a later pass"
                );
                Ok(ReclaimOutcome::ReclaimFailed)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_published_name_is_the_caller_supplied_identifier() {
        // The orphan-recovery contract: the caller records this identifier
        // as a publish intent *before* the write, so it must reach the
        // message name unchanged or a lost publish is unfindable.
        let id = crate::backend::new_publish_id();

        assert_eq!(publish_name(&id), id);
    }

    #[test]
    fn a_publish_id_carries_no_user_data() {
        // The identifier is published in a Telegram chat, so it must not
        // embed the bucket or key the object was written for. The generator
        // takes no such arguments at all; this guards that against a later
        // change to the format.
        let bucket = "customer-private-bucket";
        let key = "invoices/2026/q3-report.pdf";
        let id = crate::backend::new_publish_id();

        assert!(!id.contains(bucket), "identifier leaks the bucket: {id}");
        assert!(!id.contains(key), "identifier leaks the key: {id}");
        assert!(
            crate::backend::is_publish_id(&id),
            "identifier is not in the published shape: {id}"
        );
    }

    #[test]
    fn a_publish_is_matched_only_by_its_exact_file_name() {
        let id = crate::backend::new_publish_id();

        assert!(
            attachment_is_publish(Some(&id), &id),
            "the name the write published under is the name to find"
        );
        assert!(
            !attachment_is_publish(None, &id),
            "a message with no attachment name cannot be this publish"
        );

        // Nothing looser than equality: a partial or folded match would let
        // the sweep delete the wrong message, which is unrecoverable.
        let other = crate::backend::new_publish_id();
        assert!(!attachment_is_publish(Some(&other), &id));
        assert!(!attachment_is_publish(Some(&id[..id.len() - 1]), &id));
        assert!(!attachment_is_publish(Some(&format!("{id}x")), &id));
        assert!(!attachment_is_publish(Some(&id.to_uppercase()), &id));
        assert!(!attachment_is_publish(Some(&format!(" {id}")), &id));
    }

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

    #[test]
    fn a_well_formed_key_resolves_to_its_message_id() {
        assert_eq!(resolve_key("42").unwrap(), 42);
    }

    #[test]
    fn a_malformed_key_is_a_fault_and_not_an_absence() {
        // A key minted by another backend reaches `read` too, and it is a
        // fault: nothing this backend wrote is absent.
        let outcome = resolve_key("not-a-message-id").unwrap_err();

        match outcome {
            ReadOutcome::Fault(BackendError::InvariantViolation { detail }) => {
                assert_eq!(detail, "key is not a message id this backend minted");
            }
            other => panic!("a malformed key must be a fault, got {other:?}"),
        }
    }

    #[test]
    fn a_message_without_media_is_a_fault_and_not_an_absence() {
        // Reachable only with a live Telegram session, so the decision is
        // covered here instead: the message exists, so its bytes cannot be
        // reported missing.
        assert!(matches!(
            ReadOutcome::from(KeyFault::NoMedia),
            ReadOutcome::Fault(BackendError::InvariantViolation { .. })
        ));
    }

    #[test]
    fn delete_of_an_unparseable_key_has_nothing_to_target() {
        // The `delete` half of the idempotency contract: a key this backend
        // cannot parse names nothing it wrote, so it is already absent and
        // deleting it succeeds without a request. Reachable only on a live
        // session for the RPC that follows, so the decision is covered here.
        assert_eq!(delete_target("42"), Some(42));
        assert_eq!(delete_target("not-a-message-id"), None);
    }

    #[test]
    fn a_fault_is_distinguishable_from_a_missing_object() {
        // The whole point of the three-way answer: an operator must be able
        // to tell a corrupt key from a deleted object in a log line.
        let fault = ReadOutcome::from(KeyFault::NoMedia);
        let missing = ReadOutcome::Missing;

        assert_ne!(format!("{fault:?}"), format!("{missing:?}"));
        assert!(matches!(fault, ReadOutcome::Fault(_)));
        assert!(matches!(missing, ReadOutcome::Missing));
    }

    #[test]
    fn a_rejected_publish_published_nothing() {
        // Telegram answered this exact request with an error, so no message
        // was created and there is no orphan to look for.
        let failure = PublishFailure::Rejected {
            rpc_error: "CHAT_WRITE_FORBIDDEN".to_owned(),
        };

        assert_eq!(
            classify_publish_failure(&failure),
            PublishOutcome::NothingPublished
        );
        // A rate limit that never cleared is the same arm: the create was
        // refused on every attempt, so nothing exists in the chat.
        let exhausted = PublishFailure::Rejected {
            rpc_error: "FLOOD_WAIT".to_owned(),
        };

        assert_eq!(
            classify_publish_failure(&exhausted),
            PublishOutcome::NothingPublished
        );
    }

    #[test]
    fn a_publish_that_ran_out_of_time_is_ambiguous() {
        // The request was dispatched; its answer may have been lost, and the
        // message may already be in the chat.
        let failure = PublishFailure::TimedOut {
            after: Duration::from_secs(120),
        };

        assert_eq!(
            classify_publish_failure(&failure),
            PublishOutcome::PossiblyPublished
        );
    }

    #[test]
    fn a_publish_lost_below_the_rpc_layer_is_ambiguous() {
        // Every layer that fails after dispatch leaves the same question
        // open, whatever it is called.
        for kind in ["io", "deserialize", "transport", "uncategorised"] {
            assert_eq!(
                classify_publish_failure(&PublishFailure::ResponseLost { kind }),
                PublishOutcome::PossiblyPublished,
                "{kind} must not claim the upload published nothing"
            );
        }
    }

    #[test]
    fn only_a_rejection_is_a_proof_that_nothing_was_published() {
        let failures = [
            PublishFailure::Rejected {
                rpc_error: "CHAT_WRITE_FORBIDDEN".to_owned(),
            },
            PublishFailure::TimedOut {
                after: Duration::from_secs(1),
            },
            PublishFailure::ResponseLost { kind: "io" },
        ];

        let outcomes: Vec<_> = failures.iter().map(classify_publish_failure).collect();

        assert_eq!(
            outcomes,
            vec![
                PublishOutcome::NothingPublished,
                PublishOutcome::PossiblyPublished,
                PublishOutcome::PossiblyPublished,
            ]
        );
    }

    #[test]
    fn an_ambiguous_publish_is_logged_with_the_identifiers_of_the_upload() {
        // The only record of an orphan: no key is returned on this path, so
        // there is no metadata row and nothing else in the system names the
        // message.
        let log = capture_log(|| {
            let err = unpublished_upload(
                PublishFault::Dispatched(PublishFailure::TimedOut {
                    after: Duration::from_secs(120),
                }),
                1_700_000_000_000,
                "0d1e2f3a-4b5c-4d6e-8f90-a1b2c3d4e5f6",
            );

            assert!(matches!(err, BackendError::Other(_)), "{err:?}");
        });

        assert!(
            log.contains("1700000000000"),
            "the log must carry the file_id: {log}"
        );
        assert!(
            log.contains("0d1e2f3a-4b5c-4d6e-8f90-a1b2c3d4e5f6"),
            "the log must carry the message name: {log}"
        );
        assert!(
            log.contains("possibly-published"),
            "the log must say the outcome may have published: {log}"
        );
    }

    #[test]
    fn a_definite_publish_failure_logs_no_orphan_warning() {
        let log = capture_log(|| {
            unpublished_upload(
                PublishFault::Dispatched(PublishFailure::Rejected {
                    rpc_error: "CHAT_WRITE_FORBIDDEN".to_owned(),
                }),
                1_700_000_000_000,
                "0d1e2f3a-4b5c-4d6e-8f90-a1b2c3d4e5f6",
            );
        });

        assert!(
            !log.contains("1700000000000"),
            "nothing was published, so there is nothing to search for: {log}"
        );
    }

    #[test]
    fn a_publish_that_never_left_the_process_keeps_its_own_error() {
        // A local fault is passed through untouched, and claims nothing was
        // published.
        let err = unpublished_upload(
            PublishFault::NotDispatched(BackendError::PoisonedLock {
                lock: "flood_guard",
            }),
            1_700_000_000_000,
            "0d1e2f3a-4b5c-4d6e-8f90-a1b2c3d4e5f6",
        );

        assert!(
            matches!(err, BackendError::PoisonedLock { lock } if lock == "flood_guard"),
            "{err:?}"
        );
    }
}

/// Collect everything `f` writes through `tracing` into a string.
///
/// A subscriber is installed for the closure only, so this needs no global
/// state and no dependency beyond the `tracing-subscriber` the binary already
/// uses.
///
/// `DEBUG` is enabled so `debug!` events are captured too; the default
/// subscriber filter would drop them and a field assertion on one would pass
/// vacuously.
#[cfg(test)]
pub(crate) fn capture_log(f: impl FnOnce()) -> String {
    use std::io;
    use std::sync::{Arc, Mutex};

    /// A `MakeWriter` that appends into the buffer it shares.
    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("the log buffer lock is never held")
                .extend_from_slice(buf);

            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buffer {
        type Writer = Buffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let buffer = Buffer(Arc::default());
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(buffer.clone())
        .finish();
    let sink = Arc::clone(&buffer.0);

    tracing::subscriber::with_default(subscriber, f);

    let bytes = sink
        .lock()
        .expect("the log buffer lock is never held")
        .clone();

    String::from_utf8_lossy(&bytes).into_owned()
}

/// The field names the crate's events carry, asserted on captured output.
///
/// These check the *names*, not the prose. An event that leaves its data in
/// the message can be read by a human and counted by nothing, which is the
/// gap this module exists to keep closed: a later edit that drops a field
/// fails here rather than silently breaking a downstream exporter.
#[cfg(test)]
mod field_vocabulary {
    use super::*;

    /// A body longer than the caller declared reports the number it was
    /// measured against, so the fault can be counted without reading prose.
    #[test]
    fn an_overlong_body_names_its_declared_size_and_error_type() {
        let log = capture_log(|| {
            let _ = overlong_body(4_096);
        });

        assert!(log.contains("declared_bytes=4096"), "{log}");
        assert!(log.contains("error.type=\"write_body_overlong\""), "{log}");
    }

    /// The orphan-publish warning is the only record that tells an operator
    /// which message to delete by hand, so every identifier it needs must be
    /// a field.
    #[test]
    fn an_unconfirmed_publish_names_what_to_delete_and_why_it_failed() {
        let log = capture_log(|| {
            let _ = unpublished_upload(
                PublishFault::Dispatched(PublishFailure::TimedOut {
                    after: Duration::from_secs(120),
                }),
                1_700_000_000_000,
                "0d1e2f3a-4b5c-4d6e-8f90-a1b2c3d4e5f6",
            );
        });

        for field in [
            "file_id",
            "name",
            "outcome",
            "error.type",
            "unpublished_upload",
        ] {
            assert!(log.contains(field), "{field} must be a field: {log}");
        }
    }

    /// A poisoned mutex disables rate-limit tracking for the life of the
    /// process, and the log is the only place it is written down.
    #[test]
    fn a_poisoned_flood_guard_names_the_lock_it_lost() {
        let log = capture_log(|| {
            let _ = poisoned_flood_guard();
        });

        assert!(log.contains("lock=\"flood_guard\""), "{log}");
        assert!(log.contains("error.type=\"poisoned_lock\""), "{log}");
    }

    /// Each part upload is reported at `debug` with its index and size, which
    /// is what makes a stalled multi-part upload diagnosable.
    #[test]
    fn every_uploaded_part_is_reported_with_its_index_and_size() {
        let log = capture_log(|| {
            let mut body = std::io::Cursor::new(vec![1u8, 2, 3]);
            let plan = BodyPlan::for_size(3);
            futures::executor::block_on(Grammers::upload_parts(
                &mut body,
                &plan,
                |_index, _bytes| futures::future::ready(Ok(())),
            ))
            .expect("a body of the declared size uploads cleanly");
        });

        assert!(log.contains("part_index=0"), "{log}");
        assert!(log.contains("bytes=3"), "{log}");
        assert!(log.contains("DEBUG"), "{log}");
    }
}

/// The `write` size contract, exercised through a fake part sink.
#[cfg(test)]
mod body_contract {
    use std::future::Ready;

    use super::*;

    /// Drives [`Grammers::upload_parts`] against a body of `body`, recording
    /// the length of every part it was handed and the concatenated bytes, so
    /// the size accounting is checked end to end with no Telegram client in it.
    #[derive(Debug)]
    struct FakeUpload {
        parts: Vec<usize>,
        bytes: Vec<u8>,
    }

    impl FakeUpload {
        /// The sink `upload_parts` drives, standing in for the Telegram RPC.
        /// The future is immediate, so what the fake observes is exactly what
        /// the caller was about to send.
        fn sink(&mut self) -> impl FnMut(i32, Vec<u8>) -> Ready<Result<(), BackendError>> + '_ {
            move |part_index, bytes| {
                assert_eq!(
                    part_index as usize,
                    self.parts.len(),
                    "parts must be numbered in order"
                );
                self.parts.push(bytes.len());
                self.bytes.extend_from_slice(&bytes);

                std::future::ready(Ok(()))
            }
        }
    }

    /// Upload `body` as a declared `size`-byte object.
    async fn upload_body(body: &[u8], size: u64) -> Result<FakeUpload, BackendError> {
        let plan = BodyPlan::for_size(size as usize);
        let mut reader = std::io::Cursor::new(body.to_vec());
        let mut fake = FakeUpload {
            parts: Vec::new(),
            bytes: Vec::new(),
        };

        Grammers::upload_parts(&mut reader, &plan, fake.sink()).await?;

        Ok(fake)
    }

    #[test]
    fn an_exact_multiple_of_the_part_size_uploads_no_short_part() {
        let plan = BodyPlan::for_size(PART_SIZE * 2);

        assert_eq!(plan.total_parts, 2);
        assert_eq!(plan.lengths, vec![PART_SIZE, PART_SIZE]);
    }

    #[test]
    fn one_byte_past_a_multiple_costs_a_one_byte_part() {
        let plan = BodyPlan::for_size(PART_SIZE * 2 + 1);

        assert_eq!(plan.total_parts, 3);
        assert_eq!(plan.lengths, vec![PART_SIZE, PART_SIZE, 1]);
    }

    #[test]
    fn a_zero_byte_body_is_uploaded_as_no_parts() {
        let plan = BodyPlan::for_size(0);

        assert_eq!(plan.total_parts, 0);
        assert!(plan.lengths.is_empty());
    }

    #[tokio::test]
    async fn a_body_of_exactly_the_declared_size_uploads_every_byte() {
        let body = vec![7u8; 64];
        let fake = upload_body(&body, 64)
            .await
            .expect("a body of the declared size");

        assert_eq!(fake.parts, vec![64]);
        assert_eq!(fake.bytes, body);
    }

    #[tokio::test]
    async fn a_body_one_byte_short_fails_and_uploads_nothing() {
        let err = upload_body(&[7u8; 63], 64)
            .await
            .expect_err("a short body must fail");

        assert!(matches!(err, BackendError::Other(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_body_past_the_declared_size_fails_and_is_not_truncated() {
        // The silent-truncation case: 65 bytes declared as 64. Before the fix
        // this returned a key naming a 64-byte object and said nothing.
        let err = upload_body(&[7u8; 65], 64)
            .await
            .expect_err("an over-long body must fail");

        assert!(matches!(err, BackendError::Other(_)), "{err:?}");
        // The declared size must survive into the source chain, which is the
        // only place the detail is written down.
        let detail = std::error::Error::source(&err)
            .expect("the io cause is kept as the error source")
            .to_string();
        assert!(detail.contains("64"), "logged detail: {detail}");
    }

    #[tokio::test]
    async fn an_over_long_body_after_a_full_final_part_is_still_rejected() {
        // The `PART_SIZE` boundary case: the plan is satisfied exactly, so only
        // the post-upload probe can catch the extra byte.
        let body = vec![7u8; PART_SIZE + 1];
        let err = upload_body(&body, PART_SIZE as u64)
            .await
            .expect_err("an over-long body must fail");

        assert!(matches!(err, BackendError::Other(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_body_one_byte_over_a_full_part_uploads_exactly_the_declared_bytes() {
        let body: Vec<u8> = (0..(PART_SIZE + 1)).map(|i| i as u8).collect();
        let fake = upload_body(&body, PART_SIZE as u64 + 1)
            .await
            .expect("a body of the declared size");

        assert_eq!(fake.parts, vec![PART_SIZE, 1]);
        assert_eq!(fake.bytes, body);
    }

    #[tokio::test]
    async fn a_zero_byte_body_with_bytes_is_over_long() {
        let err = upload_body(&[7u8; 1], 0)
            .await
            .expect_err("an over-long body must fail");

        assert!(matches!(err, BackendError::Other(_)), "{err:?}");
    }
}

/// Rate-limit observability: the events a flood produces, and the attributes
/// the instruments are recorded under.
///
/// The flood guard is process-wide, so an operator tuning
/// `TELE_S3_MAX_CONCURRENT_REQUESTS`, `TELE_S3_IO_TIMEOUT_SECS` and
/// `TELE_S3_MAX_ATTEMPTS` needs to know *which* request drew a 420, *what*
/// Telegram called it and *which chat* it came from. Before this module those
/// three facts were either absent or folded into `operation`, and a rate
/// limit that named no wait produced no event at all.
#[cfg(test)]
mod flood_telemetry {
    use super::*;

    /// A `FLOOD_PREMIUM_WAIT_600` answered with no wait value: the anomalous
    /// case that used to vanish into a genericised `BackendError::Other`.
    #[test]
    fn a_rate_limit_without_a_wait_window_is_still_logged() {
        let log = capture_log(|| {
            log_flood_without_duration(
                "publish_message",
                "-1001234567890",
                "FLOOD_PREMIUM_WAIT_X",
                420,
            );
        });

        assert!(log.contains("WARN"), "{log}");
        assert!(
            log.contains("telegram_error=\"FLOOD_PREMIUM_WAIT_X\""),
            "{log}"
        );
        assert!(log.contains("rpc_error_code=420"), "{log}");
        assert!(log.contains("peer=\"-1001234567890\""), "{log}");
        assert!(log.contains("operation=\"publish_message\""), "{log}");
    }

    /// The Telegram error name gets its own field. A query has to be able to
    /// tell "we were blocked while writing" from "Telegram replied
    /// FLOOD_PREMIUM_WAIT", and it cannot if both land in `operation`.
    #[test]
    fn the_telegram_error_name_is_never_the_operation_field() {
        let log = capture_log(|| {
            log_flood_hit(
                "download_chunk",
                "-1001234567890",
                "FLOOD_PREMIUM_WAIT_600",
                420,
                Duration::from_secs(601),
            );
        });

        assert!(log.contains("operation=\"download_chunk\""), "{log}");
        assert!(
            log.contains("telegram_error=\"FLOOD_PREMIUM_WAIT_600\""),
            "{log}"
        );
        assert!(
            !log.contains("operation=\"FLOOD_PREMIUM_WAIT_600\""),
            "the Telegram error name must not be overloaded onto operation: {log}"
        );
    }

    /// Both names at once, which is the whole point: an operator reading one
    /// event sees that a *download* was blocked *by a premium wait*, not that
    /// some operation happened to be called `FLOOD_PREMIUM_WAIT_600`.
    #[test]
    fn a_flood_event_names_the_in_flight_operation_and_the_peer() {
        let log = capture_log(|| {
            log_flood_hit(
                "write",
                "-1009876543210",
                "FLOOD_WAIT_30",
                420,
                Duration::from_secs(31),
            );
        });

        assert!(log.contains("operation=\"write\""), "{log}");
        assert!(log.contains("peer=\"-1009876543210\""), "{log}");
        assert!(log.contains("telegram_error=\"FLOOD_WAIT_30\""), "{log}");
        assert!(log.contains("duration_ms=31000"), "{log}");
    }

    /// The wait an operation sits through is reported as an operation plus a
    /// peer, so a trace shows *what was blocked*, not merely that something
    /// was.
    #[test]
    fn a_flood_wait_names_what_it_delayed() {
        let log = capture_log(|| {
            log_flood_wait("delete_messages", "-1001234567890", Duration::from_secs(30));
        });

        assert!(log.contains("operation=\"delete_messages\""), "{log}");
        assert!(log.contains("peer=\"-1001234567890\""), "{log}");
        assert!(log.contains("duration_ms=30000"), "{log}");
    }

    /// The metric attribute sets, asserted directly. These are the names an
    /// operator's collector binds to, so a rename here is a breaking change
    /// and this test is what makes it a deliberate one.
    #[test]
    fn the_flood_counter_is_broken_down_by_error_name_and_operation() {
        let ordinary = flood_limit_attributes("FLOOD_WAIT_30", "write");
        let premium = flood_limit_attributes("FLOOD_PREMIUM_WAIT_600", "download_chunk");

        assert_eq!(
            ordinary[0],
            KeyValue::new("telegram_error", "FLOOD_WAIT_30".to_owned())
        );
        assert_eq!(ordinary[1], KeyValue::new("operation", "write".to_owned()));

        // A premium wait is a qualitatively different problem from an
        // ordinary one, and the breakdown has to keep them apart.
        assert_ne!(ordinary[0], premium[0]);
        assert_ne!(ordinary[1], premium[1]);
    }

    #[test]
    fn the_wait_histogram_is_broken_down_by_operation_and_peer() {
        let attrs = flood_wait_attributes("delete_messages", "-1001234567890");

        assert_eq!(
            attrs[0],
            KeyValue::new("operation", "delete_messages".to_owned())
        );
        assert_eq!(attrs[1], KeyValue::new("peer", "-1001234567890".to_owned()));
    }

    /// The RPC instruments are grouped by operation and outcome, so a
    /// timeout on one operation cannot hide behind an `ok` on another.
    #[test]
    fn the_rpc_instruments_are_broken_down_by_operation_and_outcome() {
        let ok = rpc_attributes("publish_message", RpcOutcome::Ok);
        let timed_out = rpc_attributes("publish_message", RpcOutcome::Timeout);

        assert_eq!(
            ok[0],
            KeyValue::new("operation", "publish_message".to_owned())
        );
        assert_eq!(ok[1], KeyValue::new("outcome", "ok".to_owned()));
        assert_eq!(timed_out[1], KeyValue::new("outcome", "timeout".to_owned()));

        // A closed vocabulary, so a collector can group on it.
        assert_eq!(RpcOutcome::RpcError.as_str(), "rpc_error");
    }

    /// Instruments build on a plain meter, with no exporter, no reader and no
    /// collector behind them — which is also what happens in a process with
    /// telemetry configured off, where the meter is the no-op one.
    #[test]
    fn instruments_build_without_an_exporter() {
        use opentelemetry::metrics::MeterProvider as _;

        let noop = opentelemetry::metrics::NoopMeterProvider::new();
        let metrics = TelegramMetrics::from_meter(&noop.meter(TELEMETRY_SCOPE));

        // Recording through them is inert and must not panic: this is the path
        // every request in a telemetry-less deployment takes.
        metrics.record_flood("FLOOD_WAIT_30", "write");
        metrics.record_flood_wait("write", "-1001234567890", Duration::from_secs(30));
        metrics.record_rpc("write", RpcOutcome::Ok, Duration::from_millis(120));
    }

    /// Every instrument name is prefixed, so a shared collector cannot confuse
    /// one of them with another library's.
    #[test]
    fn every_instrument_is_namespaced_to_this_crate() {
        for name in [
            FLOOD_LIMIT_COUNTER,
            FLOOD_WAIT_HISTOGRAM,
            RPC_CALL_COUNTER,
            RPC_DURATION_HISTOGRAM,
        ] {
            assert!(name.starts_with("tele_s3."), "{name} is not namespaced");
        }
    }
}
