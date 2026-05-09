use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use digest::DynDigest;
use futures::io::AsyncRead;

pub struct ReaderWithHasher<R, H>
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
