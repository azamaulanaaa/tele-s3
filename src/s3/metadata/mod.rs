use std::fmt::{Debug, Display};

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
}
}
