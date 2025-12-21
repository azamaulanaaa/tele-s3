use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use async_trait::async_trait;
use digest::DynDigest;
use futures::io::AsyncRead;

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
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError>;

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError>;

    async fn delete(&self, key: String) -> Result<(), BackendError>;
}

#[async_trait]
pub trait BackendExt: Backend {
    async fn write_with_hasher<H>(
        &self,
        size: u64,
        reader: BoxedAsyncReader,
        hasher: H,
    ) -> Result<(String, String), BackendError>
    where
        H: DynDigest + Send + 'static,
    {
        let hasher = Arc::new(Mutex::new(hasher));

        let id = {
            let reader_with_hash = ReaderWithHasher::new(reader, hasher.clone());

            let id = self.write(size, Box::pin(reader_with_hash)).await?;

            id
        };

        let mut hasher = hasher
            .lock()
            .map_err(|_| BackendError::Other("Hasher poisoned".into()))?;

        let hash = hasher
            .finalize_reset()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>();

        Ok((id, hash))
    }
}

impl<T: Backend> BackendExt for T {}

struct ReaderWithHasher<R, H>
where
    R: AsyncRead + Unpin,
    H: DynDigest,
{
    inner: R,
    hasher: Arc<Mutex<H>>,
}

impl<R, H> ReaderWithHasher<R, H>
where
    R: AsyncRead + Unpin,
    H: DynDigest,
{
    pub fn new(inner: R, hasher: Arc<Mutex<H>>) -> Self {
        Self { inner, hasher }
    }
}

impl<R, H> AsyncRead for ReaderWithHasher<R, H>
where
    R: AsyncRead + Unpin,
    H: DynDigest,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        let result = Pin::new(&mut this.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(n)) = result {
            let hasher = &mut this
                .hasher
                .lock()
                .map_err(|_| std::io::Error::other("Hasher poisoned"))?;
            hasher.update(&buf[..n]);
        }

        result
    }
}
