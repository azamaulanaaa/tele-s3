use std::ops::Deref;

use sea_orm::DatabaseConnection;
pub use sea_orm::DbErr;

pub mod entity;

pub struct Database {
    inner: DatabaseConnection,
}

impl Database {
    pub async fn init(uri: &str) -> Result<Self, DbErr> {
        let connection = sea_orm::Database::connect(uri).await?;

        Self::sync_grammers_table(&connection).await?;

        Ok(Self { inner: connection })
    }

    async fn sync_grammers_table(connection: &DatabaseConnection) -> Result<(), DbErr> {
        connection
            .get_schema_builder()
            .register(entity::dc_home::Entity)
            .register(entity::dc_option::Entity)
            .register(entity::peer_info::Entity)
            .register(entity::update_state::Entity)
            .register(entity::channel_state::Entity)
            .apply(connection)
            .await
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
