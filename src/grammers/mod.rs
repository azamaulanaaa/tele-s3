use std::{fmt::Display, path::Path, sync::Arc};

use futures::Stream;
use grammers_client::{
    Client, InputMessage,
    client::updates::UpdateStream,
    session::storages::SqliteSession,
    types::{Media, User},
};
pub use grammers_client::{
    client::files::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE},
    types::{Peer, media::Document},
};
use grammers_mtsender::{SenderPool, SenderPoolHandle};
use tokio::{io::AsyncRead, task::JoinHandle};

#[derive(Debug, thiserror::Error)]
pub enum GrammersErrorKind {
    #[error("Session file: {0}")]
    SessionFile(&'static str),
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
    #[error("Other: {0}")]
    Other(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub struct GrammersError {
    pub kind: GrammersErrorKind,
    #[source]
    pub source: Option<Box<dyn std::error::Error>>,
}

impl Display for GrammersError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

pub struct Grammers {
    session: Arc<SqliteSession>,
    sender_pool_handle: SenderPoolHandle,
    sender_pool_runner_handle: JoinHandle<()>,
    client: Client,
    update_stream: UpdateStream,
    user: Option<User>,
}

impl Grammers {
    fn new(api_id: i32, session_file: &Path) -> Result<Self, GrammersError> {
        let session = {
            let session = SqliteSession::open(session_file).map_err(|e| GrammersError {
                kind: GrammersErrorKind::SessionFile("Something wrong with it"),
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
        let sender_pool_runner_handle = tokio::spawn(async { runner.run().await });
        let update_stream = client.stream_updates(updates, Default::default());

        Ok(Self {
            session,
            sender_pool_handle: handle,
            sender_pool_runner_handle,
            client,
            update_stream,
            user: None,
        })
    }

    pub async fn init(api_id: i32, session_file: &Path) -> Result<Self, GrammersError> {
        let mut grammers = Self::new(api_id, session_file)?;

        let is_authorized = grammers
            .client
            .is_authorized()
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Other("Unable to check authorization"),
                source: Some(Box::new(e)),
            })?;
        if is_authorized {
            let user = grammers.client.get_me().await.map_err(|e| GrammersError {
                kind: GrammersErrorKind::Other("Unable to get user as me"),
                source: Some(Box::new(e)),
            })?;
            grammers.user = Some(user);
        }

        Ok(grammers)
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
        bot_api_hash: &str,
    ) -> Result<(), GrammersError> {
        let user = self
            .client
            .bot_sign_in(bot_token, bot_api_hash)
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
        document: &Document,
        chunk_size: i32,
    ) -> Result<impl Stream<Item = Result<Vec<u8>, GrammersError>>, GrammersError> {
        if !(chunk_size >= MIN_CHUNK_SIZE
            && chunk_size <= MAX_CHUNK_SIZE
            && chunk_size % MIN_CHUNK_SIZE == 0)
        {
            return Err(GrammersError {
                kind: GrammersErrorKind::InvalidConfig("Invalid download chunk size"),
                source: None,
            });
        }

        let download_iter = self.client.iter_download(document).chunk_size(chunk_size);

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
    ) -> Result<Document, GrammersError> {
        let uploaded = self
            .client
            .upload_stream(stream, size, name)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Upload("Unable to upload file"),
                source: Some(Box::new(e)),
            })?;

        let message = InputMessage::new().file(uploaded).silent(true);

        let message = self
            .client
            .send_message(peer, message)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::Upload("Unable to send message"),
                source: Some(Box::new(e)),
            })?;

        let media = message.media().ok_or(GrammersError {
            kind: GrammersErrorKind::Upload("Unable to find media from message sent"),
            source: None,
        })?;

        let document = match media {
            Media::Document(v) => v,
            _ => {
                return Err(GrammersError {
                    kind: GrammersErrorKind::Upload("The media is not a document"),
                    source: None,
                });
            }
        };

        Ok(document)
    }

    pub async fn get_peer_by_username(
        &self,
        username: &str,
    ) -> Result<Option<Peer>, GrammersError> {
        let peer = self
            .client
            .resolve_username(username)
            .await
            .map_err(|e| GrammersError {
                kind: GrammersErrorKind::PeerResolve("Unable to resolve per by username"),
                source: Some(Box::new(e)),
            })?;

        Ok(peer)
    }
}
