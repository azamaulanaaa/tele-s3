use std::pin::Pin;

use async_trait::async_trait;
use futures::io::AsyncRead;

pub use chain_readers::ChainReaders;
pub use grammers::{Grammers, GrammersConfig, GrammersLimits};
pub use memory::Memory;
pub use reader_with_hasher::{IntegrityDigests, IntegrityReader, ReaderWithHasher};

mod chain_readers;
mod grammers;
mod memory;
mod reader_with_hasher;

pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Failure of a [`Backend`] operation.
///
/// `#[non_exhaustive]` because this is the crate's public extension surface:
/// third parties implement [`Backend`] and construct these variants, so a new
/// variant must not be a silent breaking change for them. The S3 mapping
/// keeps its own catch-all arm for the same reason.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BackendError {
    #[error("Out of range")]
    OutOfRange,
    #[error("Backend is rate limited")]
    SlowDown,
    #[error("Size {actual} exceeds limit {max}")]
    ExceedLimitSize { max: u64, actual: u64 },
    /// An internal `std::sync::Mutex` was poisoned by a panic while it was
    /// held, so whatever it guards is of unknown consistency.
    ///
    /// Permanent for the life of the process: retrying cannot clear a
    /// poisoned lock, so this must never be reported as a retryable failure.
    #[error("Internal lock `{lock}` is poisoned")]
    PoisonedLock { lock: &'static str },
    /// The backend contradicted its own bookkeeping (`detail` says which
    /// invariant). Not an I/O failure and not recoverable at runtime: the
    /// backend state is corrupt and the operation cannot be served correctly.
    #[error("Backend invariant violated: {detail}")]
    InvariantViolation { detail: &'static str },
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
