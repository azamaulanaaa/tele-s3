use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;

pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Sync + Unpin>>;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("Out of range")]
    OutOfRange,
    #[error("Size {actual} exceed limit {max}")]
    ExceedLimitSize { max: u64, actual: u64 },
    #[error("Unrecognize error")]
    Other(#[source] Box<dyn std::error::Error>),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError>;

    async fn read(&self, key: &str) -> Result<Option<BoxedAsyncReader>, BackendError>;

    async fn delete(&self, key: &str) -> Result<(), BackendError>;
}
