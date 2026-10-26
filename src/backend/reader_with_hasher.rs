use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use digest::{Digest, DynDigest};
use futures::io::AsyncRead;

/// One read pass computes the streaming digests.
///
/// The md5 digest is always needed for the `ETag`. The code compares the other digests
/// only when the client sends the matching `checksum_*` header. The code
/// computes all digests in every case. This keeps the read path single-pass.
/// It also keeps the read path cheap compared with the Telegram upload.
#[derive(Debug, Default)]
pub struct IntegrityDigests {
    md5: md5::Md5,
    sha1: sha1::Sha1,
    sha256: sha2::Sha256,
    crc32: crc32fast::Hasher,
    crc32c: u32,
}

impl IntegrityDigests {
    /// Update all digests with the given bytes.
    pub fn update(&mut self, bytes: &[u8]) {
        Digest::update(&mut self.md5, bytes);
        Digest::update(&mut self.sha1, bytes);
        Digest::update(&mut self.sha256, bytes);
        self.crc32.update(bytes);
        self.crc32c = crc32c::crc32c_append(self.crc32c, bytes);
    }

    /// Finalize MD5. Reset its state.
    pub fn finalize_md5_reset(&mut self) -> Vec<u8> {
        Digest::finalize_reset(&mut self.md5).to_vec()
    }

    /// Finalize SHA-1.
    #[must_use]
    pub fn finalize_sha1(&self) -> Vec<u8> {
        self.sha1.clone().finalize().to_vec()
    }

    /// Finalize SHA-256.
    #[must_use]
    pub fn finalize_sha256(&self) -> Vec<u8> {
        self.sha256.clone().finalize().to_vec()
    }

    /// Finalize CRC32.
    #[must_use]
    pub fn finalize_crc32(&self) -> u32 {
        self.crc32.clone().finalize()
    }

    /// Finalize CRC32C.
    #[must_use]
    pub fn finalize_crc32c(&self) -> u32 {
        self.crc32c
    }
}

#[allow(missing_debug_implementations)]
/// Integrity reader.
pub struct IntegrityReader<R>
where
    R: AsyncRead + Unpin,
{
    inner: R,
    state: Arc<Mutex<IntegrityDigests>>,
}

impl<R> IntegrityReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Create a new integrity reader.
    pub fn new(inner: R, state: Arc<Mutex<IntegrityDigests>>) -> Self {
        Self { inner, state }
    }
}

impl<R> AsyncRead for IntegrityReader<R>
where
    R: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        let result = Pin::new(&mut this.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(n)) = result {
            let state = &mut this
                .state
                .lock()
                .map_err(|_| std::io::Error::other("Hasher poisoned"))?;
            state.update(&buf[..n]);
        }

        result
    }
}

#[allow(missing_debug_implementations)]
/// Reader with hasher.
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
    /// Create a new reader with hasher.
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
