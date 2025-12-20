use async_trait::async_trait;
use futures::io::AsyncRead;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

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

#[async_trait]
pub trait BackendExt: Backend {
    async fn write_with_hasher<H: Hasher + 'static>(
        &self,
        name: String,
        size: u64,
        reader: BoxedAsyncReader,
        hasher: H,
    ) -> Result<(String, String), BackendError> {
        let hasher = Arc::new(Mutex::new(hasher));

        let id = {
            let hashing_reader = ReaderWithHasher::new(reader, hasher.clone());

            let id = self.write(name, size, Box::pin(hashing_reader)).await?;

            id
        };

        let mut hasher = hasher
            .lock()
            .map_err(|_| BackendError::Other("Hasher poisoned".into()))?;

        Ok((id, hasher.finalize()))
    }
}

impl<T: Backend> BackendExt for T {}

pub trait Hasher: Send + Sync {
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> String;
}

impl Hasher for md5::Context {
    fn update(&mut self, data: &[u8]) {
        self.consume(data);
    }
    fn finalize(&mut self) -> String {
        format!("{:x}", self.clone().finalize())
    }
}

struct ReaderWithHasher<R: AsyncRead + Unpin, H: Hasher> {
    inner: R,
    hasher: Arc<Mutex<H>>,
}

impl<R: AsyncRead + Unpin, H: Hasher> ReaderWithHasher<R, H> {
    pub fn new(inner: R, hasher: Arc<Mutex<H>>) -> Self {
        Self { inner, hasher }
    }
}

impl<R: AsyncRead + Unpin, H: Hasher> AsyncRead for ReaderWithHasher<R, H> {
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
