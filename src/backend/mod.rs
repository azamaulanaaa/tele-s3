use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;

pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Sync + Unpin>>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum BackendError {}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError>;

    async fn read(&self, key: &str) -> Result<Option<BoxedAsyncReader>, BackendError>;

    async fn delete(&self, key: &str) -> Result<(), BackendError>;
}
