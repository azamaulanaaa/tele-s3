use std::{fmt::Display, path::Path, sync::Arc};

use futures::Stream;
use grammers_client::{Client, session::storages::SqliteSession, types::User};
pub use grammers_client::{
    client::files::{MAX_CHUNK_SIZE, MIN_CHUNK_SIZE},
    types::Media,
};
use grammers_mtsender::SenderPool;

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
    pool: SenderPool,
    client: Client,
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

        Ok(Self {
            session,
            pool,
            client,
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

    pub async fn download_media(
        &self,
        media: &Media,
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

        let download_iter = self.client.iter_download(media).chunk_size(chunk_size);

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
}
