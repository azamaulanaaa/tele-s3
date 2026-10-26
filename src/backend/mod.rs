use std::fmt;
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

/// Fixed prefix of every `publish_id`. An operator or a later reconciler
/// can recognise our message in the chat by this prefix alone.
pub const PUBLISH_ID_PREFIX: &str = "s3pub-";

/// Mint the opaque, caller-owned identifier that names one publish.
///
/// The S3 layer generates this before it calls [`Backend::write`].
/// It records the identifier in a durable intent row.
/// A write whose response is lost stays findable afterwards by this exact
/// string.
/// The backend must publish the object under this identifier.
/// For `Grammers`, this identifier is the file name of the Telegram message.
/// The identifier must carry no user data.
/// Clients choose bucket names and keys.
/// This string is visible in the chat.
#[must_use]
pub fn new_publish_id() -> String {
    format!("{PUBLISH_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

/// Whether `id` has the shape [`new_publish_id`] mints.
///
/// This function is pure. Backends whose keys are caller-supplied use it.
/// Without it, they cannot tell a key they received from a key they
/// could never address. That distinction is the basis for
/// [`ReadOutcome::Fault`].
#[must_use]
pub fn is_publish_id(id: &str) -> bool {
    id.strip_prefix(PUBLISH_ID_PREFIX)
        .is_some_and(|suffix| suffix.len() == 32 && suffix.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// What reclaiming an unconfirmed publish found.
///
/// The name describes the outcome, not the mechanism. A backend may answer
/// by searching an index. A backend may answer by scanning history. A
/// backend like [`Memory`] may answer because it keeps nothing outside
/// itself. The caller may rely only on what each arm says.
///
/// The enum is exhaustive on purpose. [`ReadOutcome`] is exhaustive for the
/// same reason. The row an intent leaves behind depends on the arm. A caller
/// must handle every case deliberately. A catch-all could pick the wrong
/// final state for a future variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReclaimOutcome {
    /// Nothing was ever published under this identifier. There is nothing to
    /// reclaim. The intent may be forgotten.
    NothingPublished,
    /// Something was published under this identifier. It has now been
    /// deleted. Nothing of that publish remains in the backend.
    Reclaimed,
    /// Something was published under this identifier. It could not be
    /// deleted. The publish is still there. The intent must stay pending.
    /// This is the one arm a caller may not treat as resolved. A later pass
    /// is the only thing that can retry the deletion.
    ReclaimFailed,
}

/// [`Backend`] uses a pinned, task-safe `AsyncRead` as its reader.
///
/// This type is an alias, not a newtype. The crate's own adapters reach a
/// backend without conversion. `StreamingBlobExt::into_boxed_reader` covers
/// the write path. A backend's reader covers the read path. No conversion
/// step occurs at each boundary.
/// `Pin<Box<_>>` absorbs stream types that are `!Unpin`. Readers are
/// constructed from those types.
/// `Send` applies because a reader outlives the call that produced it.
/// A reader is polled from the task that handles the response body.
/// `Unpin` on the pointee lets a consumer poll it through `&mut` with
/// `AsyncReadExt`. That consumer does not pin again.
pub type BoxedAsyncReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Failure of a [`Backend`] operation.
///
/// This enum is `#[non_exhaustive]`. This is the crate's public extension
/// surface. Third parties implement [`Backend`]. Third parties construct
/// these variants. A new variant must not be a silent breaking change for
/// them. The S3 mapping keeps its own catch-all arm for the same reason.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BackendError {
    /// The requested byte range is not satisfiable.
    #[error("Out of range")]
    OutOfRange,
    /// The backend asked the caller to slow down.
    #[error("Backend is rate limited")]
    SlowDown,
    /// The object is larger than the backend's size limit.
    #[error("Size {actual} exceeds limit {max}")]
    ExceedLimitSize {
        /// Maximum allowed size.
        max: u64,
        /// Actual size received.
        actual: u64,
    },
    /// A panic poisoned an internal `std::sync::Mutex` while it was held.
    /// The guarded state has unknown consistency.
    ///
    /// This condition is permanent for the life of the process. Retrying
    /// cannot clear a poisoned lock. This must never be reported as a
    /// retryable failure.
    #[error("Internal lock `{lock}` is poisoned")]
    PoisonedLock {
        /// Name of the poisoned lock.
        lock: &'static str,
    },
    /// The backend contradicted its own bookkeeping. `detail` says which
    /// invariant.
    /// This is not an I/O failure. This is not recoverable at runtime. The
    /// backend state is corrupt. The operation cannot be served correctly.
    #[error("Backend invariant violated: {detail}")]
    InvariantViolation {
        /// Which invariant was violated.
        detail: &'static str,
    },
    /// An unrecognized backend error.
    #[error("Unrecognize error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// What [`Backend::read`] found at a key.
///
/// This is an explicit three-way answer, not `Option`. "No object is there"
/// and "something is there but cannot be read" are different facts. A caller
/// must decide what each fact means. `Option` could not express that. `Option`
/// let an answer carry a fault as easily as an absence.
///
/// The enum is exhaustive on purpose. A caller must handle every case
/// deliberately. No catch-all arm may hide a future case. Adding a variant is
/// a breaking change. Make that decision deliberately, not by accident.
///
/// A `Fault` carries a [`BackendError`] for the same reason. The detail tells
/// an operator reading the logs whether the object was deleted. The detail
/// also tells whether the storage is corrupt.
pub enum ReadOutcome {
    /// An object is stored at the key. The reader streams it.
    Present(BoxedAsyncReader),
    /// The backend holds no object at this key. The backend itself minted
    /// this key. The object it named is gone.
    Missing,
    /// The key names something the backend cannot serve as bytes. Either the
    /// key is not one this backend can address, or the record it resolves to
    /// carries no payload. The object is not absent. Reading it failed.
    Fault(BackendError),
}

impl fmt::Debug for ReadOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // A reader has no useful `Debug`, and printing one would
            // consume or reformat the stream it wraps.
            ReadOutcome::Present(_) => f.write_str("Present(<reader>)"),
            ReadOutcome::Missing => f.write_str("Missing"),
            ReadOutcome::Fault(err) => write!(f, "Fault({err})"),
        }
    }
}

/// Storage for opaque byte objects.
/// They are addressed by the opaque keys [`Backend::write`] returns.
/// Each key is derived from the publish identifier its caller supplied.
///
/// This is the crate's extension surface. The S3 layer above assumes only
/// what is stated here.
///
/// A key is opaque. The backend never parses it. The backend stores it
/// verbatim as the `s3_blob.id` primary key. The backend reference-counts
/// it. The backend returns it
/// to `read` and `delete`. A backend must accept exactly the strings it
/// produced. A backend must not issue a key that collides with another live
/// key. The layer counts references. It upserts on conflict. Two objects that
/// share a key silently share one blob and one refcount. Each then reads the
/// other's bytes.
///
/// Calls race. The layer sends one call per blob. Those calls include a
/// `DeleteObjects` and a multi-part `GET`. An implementation must tolerate
/// concurrent calls on distinct keys. An implementation must not assume a
/// caller serializes them.
#[async_trait]
pub trait Backend: Send + Sync + 'static {
    /// Store `reader`'s bytes under the object named by `publish_id`. Return
    /// the key that names it.
    ///
    /// The caller chooses `publish_id` (see [`new_publish_id`]).
    /// It is the one coordinate that survives a failed write.
    /// The caller records it in a durable intent row before calling this.
    /// An object whose response never arrives stays findable by
    /// this string.
    /// An implementation must use it verbatim as the name the object is
    /// published under. For `Grammers`, that name is the file name of the
    /// message.
    /// An implementation must not derive the published name from anything
    /// else.
    /// It must never be parsed.
    /// It must never be embedded in a returned key.
    ///
    /// `size` is the authoritative object length, not a hint.
    /// It decides how capacity is tracked.
    /// It decides the recorded `s3_blob.size`.
    /// It decides how every later `read` calculates ranges.
    /// The reader must yield exactly that many bytes.
    /// A short body is an error. The backend returns the capacity.
    /// An over-long body is an error too.
    /// A silently truncated object must never report success.
    /// A backend that cannot see the end of the stream without reading past
    /// `size` should check for the extra byte immediately afterwards.
    /// It can still fail then without having published anything.
    ///
    /// The key must resolve for as long as the metadata store may reference
    /// it.
    /// For a durable backend, that means across restarts.
    /// A key that only works while the process lives is usable only behind a
    /// backend whose objects are as short-lived.
    ///
    /// A failure after the bytes have landed leaves an object. No returned
    /// key names that object.
    /// Nothing above can clean it up.
    /// The reconciler drops metadata rows, not backend objects.
    /// Keep that window as small as the backend allows.
    async fn write(
        &self,
        publish_id: String,
        size: u64,
        reader: BoxedAsyncReader,
    ) -> Result<String, BackendError>;

    /// Open a byte range of `key`. Report what is there, or what is not.
    ///
    /// `offset` is absolute from the start of the object.
    /// `limit` is a byte count relative to `offset`, not an end offset.
    /// A range beyond the end is not an error.
    /// The reader ends early in that case.
    /// A range that starts past the end yields an empty one.
    /// Only a range the backend cannot address at all is
    /// [`BackendError::OutOfRange`]. That happens when `offset` or `limit`
    /// is wider than the backend's own arithmetic.
    ///
    /// The answer is [`ReadOutcome`]. The variant an implementation returns
    /// is fixed.
    /// [`ReadOutcome::Present`] means an object is stored at the key. The
    /// implementation returns the reader unchanged.
    /// [`ReadOutcome::Missing`] means the key is one this backend minted. The
    /// object it named is gone. Nothing else.
    /// [`ReadOutcome::Fault`] means the key is not one this backend can
    /// address. It cannot be parsed as one of its own. That includes a key
    /// minted by another backend. Or the record it resolves to exists but
    /// carries no payload.
    ///
    /// An implementor must never report a fault as an absence.
    /// A corrupt or unrecognizable key is [`ReadOutcome::Fault`].
    /// An object that is present but unreadable is [`ReadOutcome::Fault`].
    /// Answering `Missing` instead tells the operator the user deleted the
    /// object. The truth is that the backend cannot serve it.
    /// Neither case may be reported as `Err`.
    /// `Err` means this call failed. That includes a transient fault, a
    /// refused permission, or an I/O error.
    /// `Err` is a different claim from "this object is broken".
    ///
    /// [`ReadOutcome::Missing`] says nothing about what the S3 layer
    /// answers. The layer resolves metadata before it calls.
    /// A `Missing` on a key whose metadata promised bytes means bytes were
    /// lost. That reaches the client as `InternalError`, never `NoSuchKey`.
    ///
    /// The returned reader is lazy. Bytes are fetched as it is polled. So
    /// [`ReadOutcome::Present`] promises a stream, not content.
    /// A failure after the response headers are already sent surfaces as an
    /// `std::io::Error` from the reader. That reaches the client as a
    /// truncated body.
    /// An implementation must propagate such an error. It must not return
    /// EOF early. A short read is indistinguishable from success.
    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<ReadOutcome, BackendError>;

    /// Remove `key`. Treat an already-absent key as success.
    ///
    /// The contract has two halves. Callers depend on both.
    ///
    /// Deleting a key that names nothing must succeed every time.
    /// Cleanup calls this from `DeleteObjects`.
    /// Cleanup calls this from a publish that failed after the bytes were
    /// written.
    /// Cleanup calls this from the start-up reconciler.
    /// Sometimes cleanup calls it twice for one key.
    /// Sometimes cleanup calls it for a key that is already gone.
    /// A backend that reported "not found" would turn routine cleanup into a
    /// stream of logged errors.
    /// It would also retain blob rows forever. The layer drops a row
    /// only on the answer it confirmed.
    /// A key this backend cannot parse is nothing it ever wrote.
    /// That key is absent by construction. Deleting it succeeds.
    /// That differs from [`Backend::read`], where the same key is a fault.
    ///
    /// `Ok(())` means gone. The bytes must really be gone. The bookkeeping
    /// row is deleted on that answer alone.
    /// An object that outlives its `Ok` is unreachable through the S3 API.
    /// Nothing will ever retry it.
    ///
    /// `Err` is reserved for the removal itself failing. That includes a
    /// transport error, a rate limit, or a refusal.
    /// `Err` must never be returned merely because the key was already
    /// absent.
    async fn delete(&self, key: String) -> Result<(), BackendError>;

    /// Determine what was published under `publish_id`. Reclaim it if
    /// anything was.
    ///
    /// `publish_id` is the identifier [`Backend::write`] was given.
    /// It is the only coordinate by which a publish whose answer was lost can
    /// still be found. The caller never received a key. Nothing above the
    /// backend knows one.
    /// An implementation must match on the published name it was asked to
    /// publish under. For `Grammers`, that name is the message's file name.
    /// It must never match on anything it derived itself.
    ///
    /// The answer is [`ReclaimOutcome`]. The caller maps each arm to a
    /// different final state for the recorded intent. So the arm an
    /// implementation returns is fixed.
    /// [`ReclaimOutcome::NothingPublished`] means nothing is there under this
    /// name. The intent is safe to drop.
    /// [`ReclaimOutcome::Reclaimed`] means something was there and is now
    /// gone. It carries the same guarantee [`Backend::delete`] gives.
    /// [`ReclaimOutcome::ReclaimFailed`] means something was there. The
    /// deletion did not happen.
    ///
    /// `Err` is reserved for "this call failed".
    /// `Err` must never be used for the third case.
    /// An `Err` says the backend could not determine.
    /// [`ReclaimOutcome::ReclaimFailed`] says it found something and could
    /// not remove it.
    /// Both leave the caller's intent pending for a later pass.
    /// They are different facts. The logs should not conflate them.
    ///
    /// A lookup must be bounded.
    /// An implementation that can only reach recent history must say so in
    /// its own documentation.
    /// A name older than that window is indistinguishable from a name that
    /// was never published.
    async fn reclaim_publish(&self, publish_id: String) -> Result<ReclaimOutcome, BackendError>;
}
