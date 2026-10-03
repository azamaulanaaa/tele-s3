use sea_orm::entity::prelude::*;

/// The durable record that an object write was *intended* for a bucket and
/// key, taken before the backend is called and resolved once it answers.
///
/// # Why this exists
///
/// A backend write is two steps: upload the bytes, then publish them under a
/// name the caller can find again. If the publish succeeds server-side and its
/// answer is lost, a real object exists holding those bytes, the caller gets
/// **no key**, and no key means no metadata row — so nothing in the system,
/// including `reconcile_orphan_blobs`, can ever locate it. The bytes are
/// invisible and permanent.
///
/// This row is what makes that state observable. It is written *before*
/// [`crate::backend::Backend::write`] is called, so it exists whether the call
/// succeeds, fails, or never returns; and `publish_id` is the name the
/// backend publishes under, so the object can be found by it afterwards.
///
/// # State machine
///
/// There are exactly two states, told apart by `resolved_at`:
///
/// - **Pending** (`resolved_at IS NULL`) — the write was attempted, or is
///   being attempted, and its outcome is not confirmed. A publish may or may
///   not have happened.
/// - **Resolved** (`resolved_at` set) — the backend returned a key, so the
///   publish is confirmed and `blob_id` names the object. Nothing further is
///   needed from this row.
///
/// Only the success path sets `resolved_at`. **A failed write deliberately
/// leaves the row pending**: that is the whole point. Clearing the intent on
/// failure would erase the only record of an object that may exist, and a
/// reconciler that finds nothing has nothing to repair.
///
/// A pending row is therefore the *only* record by which an object that was
/// published-but-unacknowledged can be found. The reconciler in
/// [`crate::s3::TeleS3::reconcile_publish_intents`] scans this table and acts
/// on it: it asks the backend what was published under `publish_id` and drops
/// the row only once the publish is settled. Nothing else scans it.
#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "s3_publish_intent")]
pub struct Model {
    /// The caller-supplied publish identifier, and the primary key. Chosen
    /// before the backend is called and carried no user data, so it is safe
    /// to publish as a name where clients can see it.
    #[sea_orm(primary_key, auto_increment = false)]
    pub publish_id: String,
    /// The bucket the object was written for.
    pub bucket_id: String,
    /// The key the object was written for.
    pub object_id: String,
    /// Declared object length, so a later reconciler knows what size to
    /// expect without a metadata row.
    pub size: u32,
    /// The backend key the write returned. `None` while the intent is
    /// pending — there is no key to record, which is the entire problem this
    /// table exists for.
    pub blob_id: Option<String>,
    /// When the write was attempted.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// When the backend confirmed the publish. `None` means pending.
    pub resolved_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ActiveModelBehavior for ActiveModel {}
