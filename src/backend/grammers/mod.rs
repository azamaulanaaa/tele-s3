use std::{
    ops::RangeInclusive,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use grammers_client::{Client, InputMessage, client::files::MAX_CHUNK_SIZE, types::Peer};
use grammers_mtsender::{InvocationError, SenderPool, SenderPoolHandle};
use sea_orm::DatabaseConnection;
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt},
    io::StreamReader,
};
use tracing::{instrument, warn};

use super::{Backend, BackendError, BoxedAsyncReader};

mod session;

const MAX_CONTENT_SIZE: usize = 1_500_000_000;
const EMULATE_FLOOD_RANGE: RangeInclusive<u64> = 0..=(60 * 3 * 1000);

type BoxedStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>;

pub struct GrammersConfig {
    pub app_id: i32,
    pub app_hash: String,
    pub bot_token: String,
    pub db: DatabaseConnection,
    pub username: String,
}

#[derive(Clone)]
pub struct Grammers {
    client: Client,
    sender_pool_handle: SenderPoolHandle,
    peer: Peer,
    flood_guard: Arc<Mutex<Option<Instant>>>,
}

impl Grammers {
    #[instrument(skip(config), level = "debug", err)]
    pub async fn init(config: GrammersConfig) -> anyhow::Result<Self> {
        let session = {
            let session = session::SessionStorage::init(config.db).await?;

            Arc::new(session)
        };
        let pool = SenderPool::new(session.clone(), config.app_id);
        let client = Client::new(&pool);

        let sender_pool_handle = {
            let SenderPool {
                runner,
                handle,
                updates,
            } = pool;
            let _ = tokio::spawn(runner.run());
            let _ = client.stream_updates(updates, Default::default());

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
            .ok_or(anyhow!("Username {} not found", &config.username))?;

        Ok(Self {
            sender_pool_handle,
            client,
            peer,
            flood_guard: Default::default(),
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
                .map_err(|_| BackendError::Other("Flood guard is poisoned".into()))?;

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

            let mut guard = self.flood_guard.lock().unwrap();
            *guard = None;
        }

        Ok(())
    }

    async fn catch_flood_error(&self, err: &InvocationError) -> Option<Duration> {
        if let InvocationError::Rpc(rpc_err) = err {
            if rpc_err.name == "FLOOD_WAIT" {
                let seconds = rpc_err.value.unwrap_or(0) as u64;
                let duration = Duration::from_secs(seconds + 1);

                let mut guard = self.flood_guard.lock().unwrap();
                *guard = Some(Instant::now() + duration);

                tracing::warn!("Hit FLOOD_WAIT. Blocking for {}s.", seconds);
                return Some(duration);
            }
        }

        None
    }

    async fn emulate_flood(&self) {
        let duration = Duration::from_millis(ran::ran_u64_range(EMULATE_FLOOD_RANGE));

        let mut guard = self.flood_guard.lock().unwrap();
        *guard = Some(Instant::now() + duration);
    }
}

#[async_trait]
impl Backend for Grammers {
    #[instrument(skip(self, reader), level = "debug", ret, err)]
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError> {
        let size: usize = size.try_into().map_err(|_e| BackendError::OutOfRange)?;

        if size > MAX_CONTENT_SIZE {
            return Err(BackendError::ExceedLimitSize {
                max: MAX_CONTENT_SIZE as u64,
                actual: size as u64,
            });
        }

        let mut compat_reader = reader.compat();
        let name = uuid::Uuid::new_v4().to_string();

        loop {
            self.check_flood_wait().await?;
            break;
        }

        let uploaded = match self
            .client
            .upload_stream(&mut compat_reader, size, name)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::Other {
                    match e.downcast::<InvocationError>() {
                        Ok(e) => {
                            self.catch_flood_error(&e).await;
                            return Err(BackendError::Other(Box::new(e)));
                        }
                        Err(e) => {
                            return Err(BackendError::Other(Box::new(e)));
                        }
                    }
                }

                return Err(BackendError::Other(Box::new(e)));
            }
        };

        let draft_message = InputMessage::new().file(uploaded).silent(true);

        let message = loop {
            self.check_flood_wait().await?;

            match self
                .client
                .send_message(self.peer.clone(), draft_message.clone())
                .await
            {
                Ok(msg) => break msg,
                Err(e) => {
                    if let Some(_wait) = self.catch_flood_error(&e).await {
                        continue;
                    }
                    return Err(BackendError::Other(Box::new(e)));
                }
            }
        };
        let message_id = message.id().to_string();

        self.emulate_flood().await;

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

        loop {
            self.check_flood_wait().await?;
            break;
        }

        let media = {
            let mut messages = self
                .client
                .get_messages_by_id(self.peer.clone(), &[message_id])
                .await
                .map_err(|e| BackendError::Other(Box::new(e)))?;

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
            let mut prefix_trim = offset % MAX_CHUNK_SIZE as usize;
            let mut bytes_remaining = limit;

            let mut download_iter = {
                let skip_chunks = offset / MAX_CHUNK_SIZE as usize;

                this.client
                    .iter_download(&media)
                    .chunk_size(MAX_CHUNK_SIZE)
                    .skip_chunks(skip_chunks as i32)
            };

            loop {
                this.check_flood_wait().await.map_err(|e| std::io::Error::other(e))?;

                let chunk_result = download_iter.next().await;

                let mut chunk = match chunk_result {
                    Ok(Some(c)) => c,
                    Ok(None) => break,
                    Err(e) => {
                        if let Some(_wait) = this.catch_flood_error(&e).await {
                            continue;
                        }
                        Err(std::io::Error::other(format!("Download failed: {}", e)))?
                    }
                };

                if prefix_trim > 0 {
                    if chunk.len() <= prefix_trim {
                        prefix_trim -= chunk.len();
                        continue;
                    }
                    chunk.drain(0..prefix_trim);
                    prefix_trim = 0;
                }

                if let Some(rem) = bytes_remaining {
                    if rem == 0 {
                        break;
                    }

                    let current_len = chunk.len() ;
                    if current_len > rem {
                        chunk.truncate(rem);
                        bytes_remaining = Some(0);
                    } else {
                        bytes_remaining = Some(rem - current_len);
                    }
                }

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

        loop {
            self.check_flood_wait().await?;

            match self
                .client
                .delete_messages(self.peer.clone(), &[message_id])
                .await
            {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    if self.catch_flood_error(&e).await.is_some() {
                        continue;
                    }

                    return Err(BackendError::Other(Box::new(e)));
                }
            }
        }
    }
}
