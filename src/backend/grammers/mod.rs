use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
pub use grammers_client::types::Peer;
use grammers_client::{Client, InputMessage, client::files::MAX_CHUNK_SIZE};
use grammers_mtsender::{SenderPool, SenderPoolHandle};
use sea_orm::DatabaseConnection;
use tokio_util::{
    compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt},
    io::StreamReader,
};
use tracing::instrument;

use super::{Backend, BackendError, BoxedAsyncReader};

mod session;

const MAX_CONTENT_SIZE: u64 = 1_000_000_000;

type BoxedStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = std::io::Result<bytes::Bytes>> + Send>>;

pub struct GrammersConfig {
    pub app_id: i32,
    pub app_hash: String,
    pub bot_token: String,
    pub db: DatabaseConnection,
    pub username: String,
}

pub struct Grammers {
    client: Client,
    sender_pool_handle: SenderPoolHandle,
    peer: Peer,
}

impl Grammers {
    #[instrument(skip(config), err)]
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
        })
    }

    #[instrument(skip(self))]
    pub fn close(self) {
        self.sender_pool_handle.quit();
    }
}

#[async_trait]
impl Backend for Grammers {
    #[instrument(skip(self, reader), ret, err)]
    async fn write(
        &self,
        name: String,
        size: u64,
        reader: BoxedAsyncReader,
    ) -> Result<String, BackendError> {
        if size > MAX_CONTENT_SIZE {
            return Err(BackendError::ExceedLimitSize {
                max: MAX_CONTENT_SIZE,
                actual: size,
            });
        }

        let mut compat_reader = reader.compat();
        let size = size as usize;

        let uploaded = self
            .client
            .upload_stream(&mut compat_reader, size, name)
            .await
            .map_err(|e| BackendError::Other(Box::new(e)))?;

        let draft_message = InputMessage::new().file(uploaded).silent(true);

        let message = self
            .client
            .send_message(self.peer.clone(), draft_message)
            .await
            .map_err(|e| BackendError::Other(Box::new(e)))?;

        Ok(message.id().to_string())
    }

    #[instrument(skip(self), err)]
    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError> {
        let message_id = {
            let numb_key = key.parse::<i32>();
            let numb_key = match numb_key {
                Ok(v) => v,
                Err(_e) => {
                    return Ok(None);
                }
            };

            numb_key
        };

        let offset: i32 = offset.try_into().map_err(|_| BackendError::OutOfRange)?;
        let limit: Option<i32> = limit
            .map(|v| v.try_into())
            .transpose()
            .map_err(|_| BackendError::OutOfRange)?;

        let message = {
            let messages = self
                .client
                .get_messages_by_id(self.peer.clone(), &[message_id])
                .await
                .map_err(|e| BackendError::Other(Box::new(e)))?;

            let message = match messages.get(0).cloned().flatten() {
                Some(v) => v,
                None => return Ok(None),
            };

            message
        };

        let media = match message.media() {
            Some(v) => v,
            None => return Ok(None),
        };

        let client = self.client.clone();

        let skip_chunks = (offset / MAX_CHUNK_SIZE) as i32;
        let download_iter = client
            .iter_download(&media)
            .chunk_size(MAX_CHUNK_SIZE)
            .skip_chunks(skip_chunks);

        let mut prefix_trim = (offset % MAX_CHUNK_SIZE) as usize;
        let mut bytes_remaining = limit;

        let stream = try_stream! {
            let mut iter = download_iter;

            while let Some(mut chunk) = iter.next().await.map_err(|e| std::io::Error::other(
                    format!("Download failed: {}", e)
                ))? {

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

                let current_len = chunk.len() as i32;
                if current_len > rem {
                    chunk.truncate(rem as usize);
                    bytes_remaining = Some(0);
                } else {
                    bytes_remaining = Some(rem - current_len);
                }
            }

            yield bytes::Bytes::from(chunk);
            }
        };

        let pinned_stream: BoxedStream = Box::pin(stream);
        let reader_compat = StreamReader::new(pinned_stream).compat();
        let reader = Box::pin(reader_compat);

        Ok(Some(reader))
    }

    #[instrument(skip(self), err)]
    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let message_id = {
            let numb_key = key.parse::<i32>();
            let numb_key = match numb_key {
                Ok(v) => v,
                Err(_e) => {
                    return Ok(());
                }
            };

            numb_key
        };

        self.client
            .delete_messages(self.peer.clone(), &[message_id])
            .await
            .map_err(|e| BackendError::Other(Box::new(e)))?;

        Ok(())
    }
}
