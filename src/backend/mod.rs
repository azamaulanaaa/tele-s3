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

/// The reader [`Backend`] speaks in: a pinned, task-safe `AsyncRead`.
///
/// An alias rather than a newtype so the crate's own adapters can be handed
/// straight to a backend — `StreamingBlobExt::into_boxed_reader` on the write
/// path, a backend's reader on the read path — with no conversion step at
/// each boundary. `Pin<Box<_>>` absorbs the `!Unpin` stream types readers are
/// built from, `Send` because a reader outlives the call that produced it and
/// is polled from whatever task the response body lands in, and `Unpin` on
/// the pointee so a consumer can poll it through `&mut` with `AsyncReadExt`
/// without pinning again.
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

/// Storage for opaque byte objects, addressed by the opaque keys
/// [`Backend::write`] mints.
///
/// This is the crate's extension surface, so the S3 layer above assumes only
/// what is stated here:
///
/// A key is opaque. It is never parsed, only stored verbatim as the
/// `s3_blob.id` primary key, reference-counted, and handed back to `read` and
/// `delete`. A backend must therefore accept back exactly the strings it
/// produced, and must not hand out a key that collides with another live
/// key: the layer's reference counting upserts on conflict, so two objects
/// sharing a key silently share one blob and one refcount, and each reads
/// back the other's bytes.
///
/// Calls race. The layer fans out one call per blob — a `DeleteObjects`, a
/// multi-part `GET` — so an implementation must tolerate concurrent calls on
/// distinct keys and must not assume a caller serializes them for it.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Store `reader`'s bytes under a freshly minted key and return it.
    ///
    /// `size` is the authoritative object length, not a hint: it decides
    /// capacity accounting, the recorded `s3_blob.size`, and the range
    /// arithmetic every later `read` performs. The reader must yield exactly
    /// that many bytes — a short body is an error and the capacity is handed
    /// back, never a silently truncated object. Bytes past `size` are neither
    /// stored nor an error.
    ///
    /// The key must resolve for as long as the metadata store may reference
    /// it, which for a durable backend means across restarts; a key that
    /// only works while the process lives is only usable behind a backend
    /// whose objects are as short-lived.
    ///
    /// A failure after the bytes have landed leaves an object that no
    /// returned key names, and nothing above can clean it up: the
    /// reconciler drops metadata *rows*, not backend objects. Keep that
    /// window as small as the backend allows.
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError>;

    /// Open a byte range of `key`, or report that no readable object is
    /// there.
    ///
    /// `offset` is absolute from the start of the object. `limit` is a byte
    /// count *relative to* `offset`, not an end offset. A range running past
    /// the end is not an error — the reader simply ends early, and a range
    /// starting past the end yields an empty one. Only a range the backend
    /// cannot address at all (`offset`/`limit` wider than its own
    /// arithmetic) is [`BackendError::OutOfRange`].
    ///
    /// `Ok(None)` means *no readable object at this key*, and deliberately
    /// does not separate the cases. The shipped backends answer `None` for a
    /// key they cannot parse as one of their own — including a key some other
    /// backend minted — for an object that is gone, and, for `Grammers`, for
    /// a message that exists but carries no media. Callers cannot recover the
    /// difference, so nothing else may be reported this way: a transient
    /// failure, a refused permission, or a corrupt-but-present object belong
    /// in `Err`. The layer resolves metadata before it calls, so it reads
    /// `None` as bytes lost after they were promised and answers
    /// `InternalError`, never `NoSuchKey`.
    ///
    /// The returned reader is lazy: bytes are fetched as it is polled, so
    /// `Ok(Some(_))` promises a stream, not content. A failure after the
    /// response headers are already sent surfaces as an `std::io::Error`
    /// from the reader and reaches the client as a truncated body, which is
    /// why an implementation must propagate such an error instead of
    /// returning EOF early — a short read is indistinguishable from success.
    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError>;

    /// Remove `key`, treating an already-absent key as success.
    ///
    /// Idempotent by contract. Cleanup calls this from `DeleteObjects`, from
    /// a publish that failed after the bytes were written, and from the
    /// start-up reconciler — sometimes twice for one key, sometimes for a
    /// key that is already gone. A backend that reported "not found" would
    /// turn routine cleanup into a stream of logged errors and leave blob
    /// rows behind forever, since the layer drops a row only on the answer it
    /// confirmed.
    ///
    /// The converse obligation: `Ok(())` must mean the bytes are really
    /// gone, because the bookkeeping row is deleted on that answer alone. An
    /// object that outlives its `Ok` is unreachable through the S3 API and
    /// nothing will ever retry it.
    async fn delete(&self, key: String) -> Result<(), BackendError>;
}
