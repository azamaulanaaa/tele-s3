//! Shared redb metadata store.
//!
//! One [`Store`] owns one redb database file. The S3 repository and the
//! Telegram session store both clone the same handle. A clone is cheap: it
//! holds an [`Arc`] to the open database.
//!
//! redb is synchronous. The S3 repository wraps every operation in
//! [`tokio::task::spawn_blocking`]. The code never holds a transaction across
//! an `.await`. The session store uses direct operations for its synchronous
//! trait methods and `spawn_blocking` for its asynchronous methods.
//!
//! Row values are JSON documents. Table keys are ordered, so prefix and range
//! scans stay possible.

use std::{path::Path, sync::Arc};

#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

/// Bucket rows, keyed by bucket name.
pub(crate) const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets");

/// Object rows, keyed by `(bucket, key, version id)`.
pub(crate) const OBJECTS: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("objects");

/// Latest version id for each `(bucket, key)`.
pub(crate) const LATEST: TableDefinition<(&str, &str), &str> = TableDefinition::new("latest");

/// Object version index. The key orders by bucket, key, newest timestamp
/// first, and version id. The value is empty.
pub(crate) const VERSIONS: TableDefinition<(&str, &str, u64, &str), ()> =
    TableDefinition::new("object_versions");

/// In-progress multipart uploads, keyed by `(bucket, key, upload id)`.
pub(crate) const MULTIPART: TableDefinition<(&str, &str, &str), &[u8]> =
    TableDefinition::new("multipart");

/// Blob rows, keyed by blob id.
pub(crate) const BLOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("blobs");

/// Publish intent rows, keyed by publish id.
pub(crate) const PUBLISH_INTENTS: TableDefinition<&str, &[u8]> =
    TableDefinition::new("publish_intents");

/// Pending publish intents, keyed by creation time and publish id.
pub(crate) const PUBLISH_INTENT_DUE: TableDefinition<(i64, &str), ()> =
    TableDefinition::new("publish_intent_due");

/// Home datacentre row. The singleton key is `0`.
pub(crate) const DC_HOME: TableDefinition<u8, &[u8]> = TableDefinition::new("session_dc_home");

/// Datacentre option rows, keyed by datacentre id.
pub(crate) const DC_OPTIONS: TableDefinition<i32, &[u8]> =
    TableDefinition::new("session_dc_options");

/// Peer info rows, keyed by dialog id.
pub(crate) const PEER_INFO: TableDefinition<i64, &[u8]> = TableDefinition::new("session_peer_info");

/// Update state row. The singleton key is `0`.
pub(crate) const UPDATE_STATE: TableDefinition<u8, &[u8]> =
    TableDefinition::new("session_update_state");

/// Channel update state rows, keyed by channel peer id.
pub(crate) const CHANNEL_STATE: TableDefinition<i64, &[u8]> =
    TableDefinition::new("session_channel_state");

/// A bucket row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BucketRow {
    /// Bucket name.
    pub id: String,
    /// Region recorded at creation.
    pub region: Option<String>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Versioning state. `Some("Enabled")` or `Some("Suspended")` when set.
    pub versioning_status: Option<String>,
}

/// An object version row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ObjectRow {
    /// Bucket that holds the object.
    pub bucket_id: String,
    /// Object key.
    pub id: String,
    /// Version id. `"null"` for an unversioned bucket.
    pub version_id: String,
    /// Whether this row is the latest version of its key.
    pub is_latest: bool,
    /// Whether this row is a delete marker.
    pub is_delete_marker: bool,
    /// Object size in bytes.
    pub size: u32,
    /// Last modification time.
    pub last_modified: DateTime<Utc>,
    /// Stored content type.
    pub content_type: Option<String>,
    /// Stored entity tag.
    pub etag: Option<String>,
    /// `x-amz-meta-*` map.
    pub user_metadata: serde_json::Value,
    /// Object tag set.
    pub tags: serde_json::Value,
    /// Client-provided checksums.
    pub checksums: serde_json::Value,
    /// Blob layout document.
    pub content: serde_json::Value,
}

/// A multipart upload state row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MultipartUploadRow {
    /// Bucket that holds the upload.
    pub bucket_id: String,
    /// Object key the upload will write.
    pub object_id: String,
    /// Upload id.
    pub upload_id: String,
    /// Content type captured at creation.
    pub content_type: Option<String>,
    /// `x-amz-meta-*` map captured at creation.
    pub user_metadata: serde_json::Value,
    /// Tag set captured at creation.
    pub tags: Option<serde_json::Value>,
    /// Part layout document.
    pub content: serde_json::Value,
}

/// A blob row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BlobRow {
    /// Blob id.
    pub id: String,
    /// Blob size in bytes.
    pub size: u32,
    /// Number of metadata references.
    pub refs: i64,
    /// Creation time.
    pub created_at: DateTime<Utc>,
}

/// A publish intent row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PublishIntentRow {
    /// Identifier the backend publishes under.
    pub publish_id: String,
    /// Bucket the object was written for.
    pub bucket_id: String,
    /// Key the object was written for.
    pub object_id: String,
    /// Declared object length.
    pub size: u32,
    /// Backend key. `None` while the intent is pending.
    pub blob_id: Option<String>,
    /// When the code attempted the write.
    pub created_at: DateTime<Utc>,
    /// When the backend confirmed the publish. `None` means pending.
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Home datacentre row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DcHomeRow {
    /// Datacentre id.
    pub dc_id: i32,
}

/// Datacentre option row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DcOptionRow {
    /// Datacentre id.
    pub dc_id: i32,
    /// IPv4 address text.
    pub ipv4: String,
    /// IPv6 address text.
    pub ipv6: String,
    /// Authentication key bytes.
    pub auth_key: Option<Vec<u8>>,
}

/// Peer info row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PeerInfoRow {
    /// Dialog id.
    pub peer_id: i64,
    /// Peer access hash.
    pub hash: Option<i64>,
    /// Peer subtype bits.
    pub subtype: Option<u8>,
}

/// Update state row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UpdateStateRow {
    /// Primary update state.
    pub pts: i32,
    /// Secondary update state.
    pub qts: i32,
    /// Update date.
    pub date: i32,
    /// Update sequence.
    pub seq: i32,
}

/// Channel update state row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChannelStateRow {
    /// Channel peer id.
    pub peer_id: i64,
    /// Channel update state.
    pub pts: i32,
}

/// Error from the redb store.
#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("database: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("transaction: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("table: {0}")]
    Table(#[from] redb::TableError),
    #[error("storage: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("commit: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("blocking task: {0}")]
    Join(#[from] tokio::task::JoinError),
}

/// A failure point that tests can trigger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// Fail a bucket read.
    BucketRead,
    /// Fail an object read.
    ObjectRead,
    /// Fail a blob reference release.
    BlobRelease,
    /// Fail a multipart state delete.
    StateDelete,
    /// Fail a publish intent update.
    PublishIntentUpdate,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct Faults {
    flags: [AtomicBool; 5],
    fail_blob_insert: std::sync::Mutex<Option<String>>,
}

/// The shared redb store.
///
/// The type is cheap to clone. Every clone refers to the same database file.
#[derive(Clone, Debug)]
pub struct Store {
    db: Arc<Database>,
    #[cfg(test)]
    faults: Arc<Faults>,
}

impl Store {
    /// Open a redb database file. The code creates the file when it does not
    /// exist.
    ///
    /// An existing SQLite database is not opened or imported. This is a clean
    /// cut.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        let store = Self {
            db: Arc::new(db),
            #[cfg(test)]
            faults: Arc::default(),
        };

        store.init_tables()?;

        Ok(store)
    }

    /// Open an isolated in-memory database. Every call returns its own
    /// database.
    pub fn in_memory() -> anyhow::Result<Self> {
        let backend = redb::backends::InMemoryBackend::new();
        let db = Database::builder().create_with_backend(backend)?;
        let store = Self {
            db: Arc::new(db),
            #[cfg(test)]
            faults: Arc::default(),
        };

        store.init_tables()?;

        Ok(store)
    }

    fn init_tables(&self) -> Result<(), StoreError> {
        let txn = self.db.begin_write()?;
        {
            txn.open_table(BUCKETS)?;
            txn.open_table(OBJECTS)?;
            txn.open_table(LATEST)?;
            txn.open_table(VERSIONS)?;
            txn.open_table(MULTIPART)?;
            txn.open_table(BLOBS)?;
            txn.open_table(PUBLISH_INTENTS)?;
            txn.open_table(PUBLISH_INTENT_DUE)?;
            txn.open_table(DC_HOME)?;
            txn.open_table(DC_OPTIONS)?;
            txn.open_table(PEER_INFO)?;
            txn.open_table(UPDATE_STATE)?;
            txn.open_table(CHANNEL_STATE)?;
        }
        txn.commit()?;

        Ok(())
    }

    /// Run `f` on the blocking thread pool.
    ///
    /// The function takes owned inputs and returns owned output. The redb
    /// database handle is cloned into the task. No transaction crosses an
    /// `.await`.
    pub(crate) async fn run<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(&Database) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: From<StoreError> + Send + 'static,
    {
        let db = Arc::clone(&self.db);
        match tokio::task::spawn_blocking(move || f(&db)).await {
            Ok(result) => result,
            Err(err) => Err(E::from(StoreError::Join(err))),
        }
    }

    /// Start a read transaction.
    pub(crate) fn read(&self) -> Result<redb::ReadTransaction, StoreError> {
        Ok(self.db.begin_read()?)
    }

    /// Start a write transaction. Test support only.
    #[cfg(test)]
    pub(crate) fn write(&self) -> Result<redb::WriteTransaction, StoreError> {
        Ok(self.db.begin_write()?)
    }

    /// Whether a test fault is active. Always false in production.
    pub(crate) fn fault(&self, fault: Fault) -> bool {
        #[cfg(test)]
        {
            self.faults.flags[fault as usize].load(Ordering::SeqCst)
        }
        #[cfg(not(test))]
        {
            let _ = fault;
            false
        }
    }

    /// Whether a test asked the next insert of `id` to fail. Always false in
    /// production.
    pub(crate) fn fail_blob_insert(&self, id: &str) -> bool {
        #[cfg(test)]
        {
            self.faults
                .fail_blob_insert
                .lock()
                .ok()
                .is_some_and(|value| value.as_deref() == Some(id))
        }
        #[cfg(not(test))]
        {
            let _ = id;
            false
        }
    }
}

/// Encode a row as JSON bytes.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
    Ok(serde_json::to_vec(value)?)
}

/// Decode a row from JSON bytes.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Invert a timestamp so an ascending scan returns newest rows first.
pub(crate) fn inverted_timestamp(timestamp: DateTime<Utc>) -> u64 {
    let millis = u64::try_from(timestamp.timestamp_millis()).unwrap_or(0);
    u64::MAX - millis
}

/// The first key that is greater than every key with `prefix`.
pub(crate) fn prefix_upper(prefix: &str) -> String {
    format!("{prefix}\u{10FFFF}")
}

#[cfg(test)]
mod test_support {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]

    use redb::ReadableTable;

    use super::*;

    impl Store {
        /// Set or clear a fault.
        pub(crate) fn set_fault(&self, fault: Fault, on: bool) {
            self.faults.flags[fault as usize].store(on, Ordering::SeqCst);
        }

        /// Fail the next insert of `id`.
        pub(crate) fn fail_blob_insert_for(&self, id: &str) {
            *self.faults.fail_blob_insert.lock().expect("fault lock") = Some(id.to_owned());
        }

        /// Insert an object row and keep both indexes in step.
        pub(crate) fn seed_object(&self, row: &ObjectRow) {
            let txn = self.write().expect("write");
            {
                let mut objects = txn.open_table(OBJECTS).expect("objects");
                let mut versions = txn.open_table(VERSIONS).expect("versions");
                let mut latest = txn.open_table(LATEST).expect("latest");
                let bytes = encode(row).expect("encode object");
                objects
                    .insert(
                        (
                            row.bucket_id.as_str(),
                            row.id.as_str(),
                            row.version_id.as_str(),
                        ),
                        bytes.as_slice(),
                    )
                    .expect("insert object");
                versions
                    .insert(
                        (
                            row.bucket_id.as_str(),
                            row.id.as_str(),
                            inverted_timestamp(row.last_modified),
                            row.version_id.as_str(),
                        ),
                        &(),
                    )
                    .expect("insert version");
                if row.is_latest {
                    latest
                        .insert(
                            (row.bucket_id.as_str(), row.id.as_str()),
                            row.version_id.as_str(),
                        )
                        .expect("insert latest");
                }
            }
            txn.commit().expect("commit");
        }

        /// Insert a blob row.
        pub(crate) fn seed_blob(&self, row: &BlobRow) {
            let txn = self.write().expect("write");
            {
                let mut table = txn.open_table(BLOBS).expect("blobs");
                let bytes = encode(row).expect("encode blob");
                table
                    .insert(row.id.as_str(), bytes.as_slice())
                    .expect("insert");
            }
            txn.commit().expect("commit");
        }

        /// Replace one blob reference count.
        pub(crate) fn set_blob_refs(&self, id: &str, refs: i64) {
            let txn = self.write().expect("write");
            {
                let mut table = txn.open_table(BLOBS).expect("blobs");
                let current = table
                    .get(id)
                    .expect("get blob")
                    .expect("blob row")
                    .value()
                    .to_vec();
                let mut row: BlobRow = decode(&current).expect("decode blob");
                row.refs = refs;
                let bytes = encode(&row).expect("encode blob");
                table.insert(id, bytes.as_slice()).expect("insert");
            }
            txn.commit().expect("commit");
        }

        /// All blob rows, ordered by id.
        pub(crate) fn all_blobs(&self) -> Vec<BlobRow> {
            let txn = self.read().expect("read");
            let table = txn.open_table(BLOBS).expect("blobs");
            table
                .iter()
                .expect("iterate")
                .map(|entry| {
                    let (_, value) = entry.expect("entry");
                    decode(value.value()).expect("decode blob")
                })
                .collect()
        }

        /// All object rows for one bucket.
        pub(crate) fn all_objects(&self, bucket: &str) -> Vec<ObjectRow> {
            let txn = self.read().expect("read");
            let table = txn.open_table(OBJECTS).expect("objects");
            table
                .range((bucket, "", "")..)
                .expect("range")
                .take_while(|entry| entry.as_ref().is_ok_and(|(key, _)| key.value().0 == bucket))
                .map(|entry| {
                    let (_, value) = entry.expect("entry");
                    decode(value.value()).expect("decode object")
                })
                .collect()
        }

        /// All publish intent rows.
        pub(crate) fn all_publish_intents(&self) -> Vec<PublishIntentRow> {
            let txn = self.read().expect("read");
            let table = txn.open_table(PUBLISH_INTENTS).expect("intents");
            table
                .iter()
                .expect("iterate")
                .map(|entry| {
                    let (_, value) = entry.expect("entry");
                    decode(value.value()).expect("decode intent")
                })
                .collect()
        }

        /// Insert a publish intent row and its due index entry when pending.
        pub(crate) fn seed_publish_intent(&self, row: &PublishIntentRow) {
            let txn = self.write().expect("write");
            {
                let mut table = txn.open_table(PUBLISH_INTENTS).expect("intents");
                let mut due = txn.open_table(PUBLISH_INTENT_DUE).expect("due");
                let bytes = encode(row).expect("encode intent");
                table
                    .insert(row.publish_id.as_str(), bytes.as_slice())
                    .expect("insert");
                if row.resolved_at.is_none() {
                    due.insert(
                        (row.created_at.timestamp_millis(), row.publish_id.as_str()),
                        &(),
                    )
                    .expect("insert due");
                }
            }
            txn.commit().expect("commit");
        }

        /// Replace the creation time of a pending intent.
        pub(crate) fn set_publish_intent_created_at(
            &self,
            publish_id: &str,
            created_at: DateTime<Utc>,
        ) {
            let txn = self.write().expect("write");
            {
                let mut table = txn.open_table(PUBLISH_INTENTS).expect("intents");
                let mut due = txn.open_table(PUBLISH_INTENT_DUE).expect("due");
                let current = table
                    .get(publish_id)
                    .expect("get intent")
                    .expect("intent row")
                    .value()
                    .to_vec();
                let mut row: PublishIntentRow = decode(&current).expect("decode intent");
                let old = row.created_at;
                row.created_at = created_at;
                let bytes = encode(&row).expect("encode intent");
                table.insert(publish_id, bytes.as_slice()).expect("insert");
                if row.resolved_at.is_none() {
                    due.remove((old.timestamp_millis(), publish_id))
                        .expect("remove old due");
                    due.insert((created_at.timestamp_millis(), publish_id), &())
                        .expect("insert due");
                }
            }
            txn.commit().expect("commit");
        }
    }
}
