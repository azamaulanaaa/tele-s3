use std::{fmt::Display, path::Path, sync::Arc};

use grammers_client::Client;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;

#[derive(Debug, thiserror::Error)]
pub enum GrammersErrorKind {
    #[error("Session file: {0}")]
    SessionFile(&'static str),
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
}

impl Grammers {
    pub fn new(api_id: i32, session_file: &Path) -> Result<Self, GrammersError> {
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
        })
    }
}
