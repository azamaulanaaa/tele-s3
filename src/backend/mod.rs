use async_trait::async_trait;
use futures::io::{AsyncRead, AsyncWrite};
use std::pin::Pin;

pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Sync + Unpin>>;
pub type BoxedAsyncWriter = Pin<Box<dyn AsyncWrite + Send + Sync + Unpin>>;

#[derive(Debug, Clone, thiserror::Error)]
pub enum BackendError {}

#[async_trait]
pub trait Backend: Send + Sync + 'static {
    async fn get_writer(&self, key: &str) -> Result<BoxedAsyncWriter, BackendError>;

    async fn get_reader(&self, key: &str) -> Result<Option<BoxedAsyncReader>, BackendError>;

    async fn delete(&self, key: &str) -> Result<(), BackendError>;
}
