use std::ops::Deref;

use sea_orm::DatabaseConnection;
pub use sea_orm::DbErr;

pub struct Database {
    inner: DatabaseConnection,
}

impl Database {
    pub async fn init(uri: &str) -> Result<Self, DbErr> {
        let connection = sea_orm::Database::connect(uri).await?;

        Self::sync_grammers_table(&connection).await?;

        Ok(Self { inner: connection })
    }
}

impl Deref for Database {
    type Target = DatabaseConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<Database> for DatabaseConnection {
    fn from(value: Database) -> Self {
        value.inner
    }
}
