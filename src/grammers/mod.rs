use std::{collections::HashMap, fmt::Display, sync::Arc};

use futures::{Stream, io::AsyncRead};
use grammers_client::{Client, InputMessage, client::updates::UpdateStream, types::User};
pub use grammers_client::{
    client::files::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE},
    types::Peer,
};
use grammers_mtsender::{SenderPool, SenderPoolHandle};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::compat::FuturesAsyncReadCompatExt;

mod session;

#[derive(Debug, thiserror::Error)]
pub enum GrammersErrorKind {
    #[error("Session Storage: {0}")]
    SessionStorage(&'static str),
    #[error("Authentication: {0}")]
    Authentication(&'static str),
    #[error("Invalid Config: {0}")]
    InvalidConfig(&'static str),
    #[error("Download: {0}")]
    Download(&'static str),
    #[error("Upload: {0}")]
    Upload(&'static str),
    #[error("Peer Resolve: {0}")]
    PeerResolve(&'static str),
    #[error("Message Resolve: {0}")]
    MsgResolve(&'static str),
    #[error("Other: {0}")]
    Other(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub struct GrammersError {
    pub kind: GrammersErrorKind,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for GrammersError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageId {
    id: i32,
}

pub struct Grammers {
    session: Arc<session::SessionStorage>,
    sender_pool_handle: SenderPoolHandle,
    sender_pool_runner_handle: JoinHandle<()>,
    client: Client,
    update_stream: UpdateStream,
    user: Option<User>,
    peer_cache: Mutex<HashMap<String, Option<Peer>>>,
}

impl Grammers {
    pub async fn init(api_id: i32, connection: DatabaseConnection) -> Result<Self, GrammersError> {
        let session = {
            let session = session::SessionStorage::init(connection)
                .await
                .map_err(|e| GrammersError {
                    kind: GrammersErrorKind::SessionStorage("unable to initiate session storage"),
                    source: Some(Box::new(e)),
                })?;

            Arc::new(session)
        };

        let pool = SenderPool::new(session.clone(), api_id);

        let client = Client::new(&pool);

        let SenderPool {
            runner,
            handle,
            updates,
        } = pool;
        let sender_pool_runner_handle = tokio::spawn(runner.run());
        let update_stream = client.stream_updates(updates, Default::default());

        let user = {
            let is_authorized = client.is_authorized().await.map_err(|e| GrammersError {
                kind: GrammersErrorKind::Other("Unable to check authorization"),
                source: Some(Box::new(e)),
            })?;

            if is_authorized {
                let user = client.get_me().await.map_err(|e| GrammersError {
                    kind: GrammersErrorKind::Other("Unable to get user as me"),
                    source: Some(Box::new(e)),
                })?;

                Some(user)
            } else {
                None
            }
        };

        Ok(Self {
            session,
            sender_pool_handle: handle,
            sender_pool_runner_handle,
            client,
            update_stream,
            user,
            peer_cache: Default::default(),
        })
    }

    pub async fn close(self: Self) {
        self.sender_pool_handle.quit();
        let _ = self.sender_pool_runner_handle.await;
    }

    pub fn is_authorized(&self) -> bool {
        self.user.is_some()
    }

    pub async fn authenticate(
        &mut self,
        bot_token: &str,
        api_hash: &str,
    ) -> Result<(), GrammersError> {
        let user = self
            .client
            .bot_sign_in(bot_token, api_hash)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Authentication("Something wrong with it"),
                source: Some(Box::new(e)),
            })?;

        self.user = Some(user);

        Ok(())
    }

    pub async fn download_document(
        &self,
        peer: Peer,
        message_id: MessageId,
        chunk_size: i32,
    ) -> Result<
        impl Stream<Item = Result<Vec<u8>, GrammersError>> + Send + Sync + 'static,
        GrammersError,
    > {
        if !(chunk_size >= MIN_CHUNK_SIZE
            && chunk_size <= MAX_CHUNK_SIZE
            && chunk_size % MIN_CHUNK_SIZE == 0)
        {
            return Err(GrammersError {
                kind: GrammersErrorKind::InvalidConfig("Invalid download chunk size"),
                source: None,
            });
        }

        let message = self
            .client
            .get_messages_by_id(peer, &[message_id.id])
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::MsgResolve("Unable to get the message"),
                source: Some(Box::new(e)),
            })?
            .get(0)
            .cloned()
            .flatten()
            .ok_or(GrammersError {
                kind: GrammersErrorKind::MsgResolve("message does not found"),
                source: None,
            })?;

        let document = message.media().ok_or(GrammersError {
            kind: GrammersErrorKind::MsgResolve("media does not found in the message"),
            source: None,
        })?;

        let client = self.client.clone();
        let download_iter = client.iter_download(&document).chunk_size(chunk_size);

        let media_download = futures::stream::unfold(download_iter, |mut this| async move {
            let result = this
                .next()
                .await
                .map_err(|e| GrammersError {
                    kind: GrammersErrorKind::Download("Something wrong"),
                    source: Some(Box::new(e)),
                })
                .transpose();

            result.map(|v| (v, this))
        });

        Ok(media_download)
    }

    pub async fn upload_document<S: AsyncRead + Unpin>(
        &self,
        stream: &mut S,
        size: usize,
        name: String,
        peer: Peer,
    ) -> Result<MessageId, GrammersError> {
        let mut compat_stream = stream.compat();

        let uploaded = self
            .client
            .upload_stream(&mut compat_stream, size, name)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Upload("Unable to upload file"),
                source: Some(Box::new(e)),
            })?;

        let message = InputMessage::new().file(uploaded).silent(true);

        let message = self
            .client
            .send_message(peer.clone(), message)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Upload("Unable to send message"),
                source: Some(Box::new(e)),
            })?;

        let message_id = MessageId { id: message.id() };

        Ok(message_id)
    }

    pub async fn get_peer_by_username(
        &self,
        username: &str,
    ) -> Result<Option<Peer>, GrammersError> {
        let peer = self
            .peer_cache
            .lock()
            .await
            .get(username)
            .cloned()
            .flatten();
        if peer.is_some() {
            return Ok(peer);
        }

        let peer = self
            .client
            .resolve_username(username)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::PeerResolve("Unable to resolve per by username"),
                source: Some(Box::new(e)),
            })?;
        let _ = self
            .peer_cache
            .lock()
            .await
            .insert(username.to_string(), peer.clone());

        Ok(peer)
    }
}
