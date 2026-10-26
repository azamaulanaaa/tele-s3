use sea_orm::entity::prelude::*;

/// This row records an intended object write for a bucket and key.
/// The code takes the row before it calls the backend. The code resolves
/// the row once the backend answers.
///
/// # Why this exists
///
/// A backend write has two steps. First, the code uploads the bytes. Then
/// the code publishes them under a name. The caller can find that name
/// again.
///
/// If the publish succeeds on the server, and the answer is lost, a real
/// object exists. That object holds those bytes. The caller gets no key.
/// No key means no metadata row. Nothing in the system can locate that
/// object. `reconcile_orphan_blobs` cannot locate it either. The bytes
/// stay invisible and permanent.
///
/// This row makes that state observable. The code writes the row before it
/// calls [`crate::backend::Backend::write`]. The row exists whether the
/// call succeeds, fails, or never returns. `publish_id` is the name the
/// backend publishes under. The code can find the object by that name
/// afterwards.
///
/// # State machine
///
/// There are exactly two states. The value of `resolved_at` distinguishes
/// them.
///
/// - **Pending** (`resolved_at IS NULL`): the code attempted the write, or
///   is attempting it. The outcome is not confirmed. A publish may or may
///   not have happened.
/// - **Resolved** (`resolved_at` set): the backend returned a key. The
///   code confirms the publish. `blob_id` names the object. This row
///   needs nothing further.
///
/// Only the success path sets `resolved_at`. A failed write leaves the row
/// pending deliberately. That is the whole point. Clearing the intent on
/// failure would erase the only record of an object that may exist. A
/// reconciler that finds nothing has nothing to repair.
///
/// A pending row is the only record that can find an object published but
/// unacknowledged. The reconciler
/// [`crate::s3::TeleS3::reconcile_publish_intents`] scans this table. It
/// processes the table. It asks the backend what it published under
/// `publish_id`. It drops the row only after the publish settles. Nothing
/// else scans this table.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_publish_intent")]
pub(crate) struct Model {
    /// The caller supplies this publish identifier. It is also the primary
    /// key. The code chooses it before it calls the backend. It carries no
    /// user data. The code can safely publish it as a name that clients can
    /// see.
    #[sea_orm(primary_key, auto_increment = false)]
    pub publish_id: String,
    /// The bucket the object was written for.
    pub bucket_id: String,
    /// The key the object was written for.
    pub object_id: String,
    /// Declared object length. A later reconciler knows what size to
    /// expect. It does not need a metadata row.
    pub size: u32,
    /// The backend key that the write returned. It is `None` while the
    /// intent is pending. There is no key to record. That missing key is
    /// the entire problem this table exists for.
    pub blob_id: Option<String>,
    /// When the code attempted the write.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the backend confirmed the publish. `None` means pending.
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ActiveModelBehavior for ActiveModel {}
