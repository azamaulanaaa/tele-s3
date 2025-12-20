use async_trait::async_trait;
use futures::io::AsyncRead;
use std::pin::Pin;

pub use grammers::{Grammers, GrammersConfig};

mod grammers;

pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("Out of range")]
    OutOfRange,
    #[error("Size {actual} exceed limit {max}")]
    ExceedLimitSize { max: u64, actual: u64 },
    #[error("Unrecognize error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn write(
        &self,
        name: String,
        size: u64,
        reader: BoxedAsyncReader,
    ) -> Result<String, BackendError>;

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError>;

    async fn delete(&self, key: String) -> Result<(), BackendError>;
}
