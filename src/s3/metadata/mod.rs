use std::{
    fmt::{Debug, Display},
    ops::Deref,
};

use sea_orm::{
    ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, Condition, DatabaseConnection, DbErr,
    EntityTrait, QueryFilter, QuerySelect, Set,
};

mod entity;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub key: String,
    pub bucket: String,
    pub size: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub content_type: Option<String>,
    pub etag: Option<String>,

    pub inner: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct MetadataSummary {
    pub key: String,
    pub size: u64,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ListResult {
    pub metadatas: Vec<MetadataSummary>,
    pub common_prefix: Vec<String>,
    pub next_token: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum MetadataStorageErrorKind {
    #[error("Database error: {0}")]
    Database(&'static str),
    #[error("Other error: {0}")]
    Other(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub struct MetadataStorageError {
    pub kind: MetadataStorageErrorKind,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for MetadataStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

#[async_trait::async_trait]
pub trait MetadataStorage: Send + Sync + 'static {
    async fn put(&self, metadata: Metadata) -> Result<(), MetadataStorageError>;
    async fn get(&self, bucket: &str, key: &str) -> Result<Option<Metadata>, MetadataStorageError>;
    async fn delete(&self, bucket: &str, key: &str) -> Result<(), MetadataStorageError>;
    async fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: u64,
        token: Option<&str>,
    ) -> Result<ListResult, MetadataStorageError>;
}

pub struct MetadataDb {
    inner: DatabaseConnection,
}

impl MetadataDb {
    pub async fn init(connection: DatabaseConnection) -> Result<Self, MetadataStorageError> {
        Self::apply_table(&connection)
            .await
            .map_err(|e| MetadataStorageError {
                kind: MetadataStorageErrorKind::Database("Unable to apply table"),
                source: Some(Box::new(e)),
            })?;

        Ok(Self { inner: connection })
    }

    async fn apply_table(connection: &DatabaseConnection) -> Result<(), DbErr> {
        connection
            .get_schema_builder()
            .register(entity::metadata::Entity)
            .apply(connection)
            .await
    }
}

impl Deref for MetadataDb {
    type Target = DatabaseConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait::async_trait]
impl MetadataStorage for MetadataDb {
    async fn put(&self, metadata: Metadata) -> Result<(), MetadataStorageError> {
        entity::metadata::ActiveModel {
            id: NotSet,
            key: Set(metadata.key),
            bucket: Set(metadata.bucket),
            size: Set(metadata.size as _),
            last_modified: Set(metadata.last_modified),
            content_type: Set(metadata.content_type),
            etag: Set(metadata.etag),
            inner: Set(metadata.inner),
        }
        .insert(&self.inner)
        .await
        .map_err(|e| MetadataStorageError {
            kind: MetadataStorageErrorKind::Database("Unable to insert object"),
            source: Some(Box::new(e)),
        })?;

        Ok(())
    }

    async fn get(&self, bucket: &str, key: &str) -> Result<Option<Metadata>, MetadataStorageError> {
        let metadata = entity::metadata::Entity::find()
            .filter(
                Condition::all()
                    .add(entity::metadata::Column::Bucket.eq(bucket))
                    .add(entity::metadata::Column::Key.eq(key)),
            )
            .one(&self.inner)
            .await
            .map_err(|e| MetadataStorageError {
                kind: MetadataStorageErrorKind::Database("Unable to get object"),
                source: Some(Box::new(e)),
            })?;
        let metadata = metadata.map(|v| Metadata {
            key: v.key,
            bucket: v.bucket,
            size: v.size as _,
            last_modified: v.last_modified,
            content_type: v.content_type,
            etag: v.etag,
            inner: v.inner,
        });

        Ok(metadata)
    }

    async fn delete(&self, bucket: &str, key: &str) -> Result<(), MetadataStorageError> {
        entity::metadata::Entity::delete_many()
            .filter(
                Condition::all()
                    .add(entity::metadata::Column::Bucket.eq(bucket))
                    .add(entity::metadata::Column::Key.eq(key)),
            )
            .exec(&self.inner)
            .await
            .map_err(|e| MetadataStorageError {
                kind: MetadataStorageErrorKind::Database("Unable to delete a metadata"),
                source: Some(Box::new(e)),
            })?;

        Ok(())
    }

    async fn list(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        limit: u64,
        token: Option<&str>,
    ) -> Result<ListResult, MetadataStorageError> {
        let condition = {
            let mut condition = Condition::all().add(entity::metadata::Column::Bucket.eq(bucket));

            if let Some(prefix) = prefix {
                condition = condition.add(entity::metadata::Column::Key.starts_with(prefix));
            }

            if let Some(token) = token {
                condition = condition.add(entity::metadata::Column::Key.gt(token));
            }

            if let Some(delimiter) = delimiter {
                let prefix = prefix.unwrap_or_default();
                let pattern = format!("{}{}%", prefix, delimiter);
                condition = condition.add(entity::metadata::Column::Key.not_like(&pattern));
            }

            condition
        };

        let metadatas = entity::metadata::Entity::find()
            .filter(condition)
            .limit(Some(limit))
            .all(&self.inner)
            .await
            .map_err(|e| MetadataStorageError {
                kind: MetadataStorageErrorKind::Database("Unable to list object"),
                source: Some(Box::new(e)),
            })?;
        let metadatas = metadatas
            .into_iter()
            .map(|v| MetadataSummary {
                key: v.key,
                size: v.size as _,
                last_modified: v.last_modified,
                etag: v.etag,
            })
            .collect::<Vec<_>>();

        let next_token = if metadatas.len() as u64 == limit {
            metadatas.last().map(|s| s.key.clone())
        } else {
            None
        };

        let common_prefix = Vec::new();

        Ok(ListResult {
            metadatas,
            common_prefix,
            next_token,
        })
    }
}
