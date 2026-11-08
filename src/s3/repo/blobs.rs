use std::{
    collections::{HashMap, HashSet},
    ops::Bound,
};

use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable, WriteTransaction};
use s3s::{S3Error, S3Result};
use tracing::instrument;

use super::{Repository, StoreResultExt};
use crate::storage::{
    BLOBS, BlobRow, Fault, MULTIPART, OBJECTS, PUBLISH_INTENT_DUE, PUBLISH_INTENTS,
    PublishIntentRow, decode, encode,
};

/// Reference count that marks a row for backend deletion.
///
/// The value stays below any count an old underflow can produce. A row left
/// negative by an old bug still reads as released. It does not read as claimed.
const BLOB_REF_DELETING: i64 = i64::MIN;

fn blob_row<T>(table: &T, id: &str) -> Result<Option<BlobRow>, crate::storage::StoreError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(id)? else {
        return Ok(None);
    };

    Ok(Some(decode(value.value())?))
}

fn publish_intent_row<T>(
    table: &T,
    publish_id: &str,
) -> Result<Option<PublishIntentRow>, crate::storage::StoreError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(publish_id)? else {
        return Ok(None);
    };

    Ok(Some(decode(value.value())?))
}

/// Insert a blob row owned by one reference on `txn`.
pub(super) fn insert_blob_in_txn(txn: &WriteTransaction, id: &str, size: u64) -> S3Result<()> {
    if txn.open_table(BLOBS).store()?.get(id).store()?.is_some() {
        return Err(S3Error::internal_error(std::io::Error::other(
            "blob row already exists",
        )));
    }

    let row = BlobRow {
        id: id.to_owned(),
        size: Repository::checked_size(size)?,
        refs: 1,
        created_at: chrono::Local::now().to_utc(),
    };
    let bytes = encode(&row).store()?;
    txn.open_table(BLOBS)
        .store()?
        .insert(id, bytes.as_slice())
        .store()?;

    Ok(())
}

/// Mark a publish intent resolved on `txn`.
pub(super) fn resolve_publish_intent_in_txn(
    txn: &WriteTransaction,
    publish_id: &str,
    blob_id: String,
) -> S3Result<()> {
    let mut table = txn.open_table(PUBLISH_INTENTS).store()?;
    let Some(mut row) = publish_intent_row(&table, publish_id).store()? else {
        return Err(S3Error::internal_error(std::io::Error::other(
            "publish intent was not pending",
        )));
    };
    if row.resolved_at.is_some() {
        return Err(S3Error::internal_error(std::io::Error::other(
            "publish intent was not pending",
        )));
    }

    row.blob_id = Some(blob_id);
    row.resolved_at = Some(chrono::Local::now().to_utc());
    let bytes = encode(&row).store()?;
    table.insert(publish_id, bytes.as_slice()).store()?;
    drop(table);

    let mut due = txn.open_table(PUBLISH_INTENT_DUE).store()?;
    due.remove((row.created_at.timestamp_millis(), publish_id))
        .store()?;

    Ok(())
}

impl Repository {
    /// Insert a blob row owned by one reference.
    #[allow(dead_code)]
    pub(super) async fn insert_blob(&self, id: String, size: u64) -> S3Result<()> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                insert_blob_in_txn(&txn, &id, size)?;
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Register a new backend blob with one reference.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn register_new_blob(&self, id: String, size: u64) -> S3Result<()> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                insert_blob_in_txn(&txn, &id, size)?;
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Record the intent to publish an object before the backend runs.
    ///
    /// The write uses the caller's own identifier (`publish_id`). An object
    /// published without an answer can still be found by that identifier.
    ///
    /// The code inserts the row up front. A failure leaves the intent on
    /// record. That includes a failure with no answer. A caller that cannot
    /// record the intent must not call the backend. An unrecorded publish is
    /// unrecoverable.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn record_publish_intent(
        &self,
        publish_id: String,
        bucket_id: String,
        object_id: String,
        size: u64,
    ) -> S3Result<()> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(PUBLISH_INTENTS).store()?;
                    if table.get(publish_id.as_str()).store()?.is_some() {
                        return Err(S3Error::internal_error(std::io::Error::other(
                            "publish intent already exists",
                        )));
                    }

                    let row = PublishIntentRow {
                        publish_id: publish_id.clone(),
                        bucket_id,
                        object_id,
                        size: Repository::checked_size(size)?,
                        blob_id: None,
                        created_at: chrono::Local::now().to_utc(),
                        resolved_at: None,
                    };
                    let bytes = encode(&row).store()?;
                    table
                        .insert(publish_id.as_str(), bytes.as_slice())
                        .store()?;

                    let mut due = txn.open_table(PUBLISH_INTENT_DUE).store()?;
                    due.insert(
                        (row.created_at.timestamp_millis(), publish_id.as_str()),
                        &(),
                    )
                    .store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Mark a recorded intent resolved after the backend returns a key.
    ///
    /// Test-only: every production success path resolves inside the publish
    /// transaction. See [`resolve_publish_intent_in_txn`].
    #[cfg(test)]
    pub(crate) async fn resolve_publish_intent(
        &self,
        publish_id: &str,
        blob_id: String,
    ) -> S3Result<()> {
        let publish_id = publish_id.to_owned();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                resolve_publish_intent_in_txn(&txn, &publish_id, blob_id)?;
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Pending publish intents older than `older_than`. Oldest first. Capped
    /// at `limit` rows.
    ///
    /// The age filter is the safety rule of the pass. An intent younger than
    /// the threshold may belong to a write still in flight. Acting on it would
    /// look for a publish that has not happened yet.
    pub(crate) async fn pending_publish_intents(
        &self,
        older_than: DateTime<Utc>,
        limit: u64,
    ) -> S3Result<Vec<PublishIntentRow>> {
        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let due = txn.open_table(PUBLISH_INTENT_DUE).store()?;
                let intents = txn.open_table(PUBLISH_INTENTS).store()?;
                let mut rows = Vec::new();

                for entry in due.range((i64::MIN, "")..).store()? {
                    let (key, _) = entry.store()?;
                    let (created_at, publish_id) = key.value();
                    if created_at >= older_than.timestamp_millis() {
                        break;
                    }
                    let Some(value) = intents.get(publish_id).store()? else {
                        continue;
                    };
                    let row: PublishIntentRow = decode(value.value()).store()?;
                    if row.resolved_at.is_none() {
                        rows.push(row);
                    }
                    if rows.len() as u64 == limit {
                        break;
                    }
                }

                Ok(rows)
            })
            .await
    }

    /// Drop the recorded intents whose publishes are settled.
    ///
    /// The check is guarded on `resolved_at IS NULL`. A write that resolves
    /// between selection and this statement keeps its row. A resolved row is
    /// the only record of which key a confirmed publish produced. The
    /// reconciler must not remove one.
    pub(crate) async fn clear_publish_intents(&self, publish_ids: &[String]) -> S3Result<()> {
        if publish_ids.is_empty() {
            return Ok(());
        }

        let publish_ids = publish_ids.to_vec();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(PUBLISH_INTENTS).store()?;
                    let mut due = txn.open_table(PUBLISH_INTENT_DUE).store()?;
                    for publish_id in &publish_ids {
                        let Some(row) = publish_intent_row(&table, publish_id).store()? else {
                            continue;
                        };
                        if row.resolved_at.is_some() {
                            continue;
                        }
                        table.remove(publish_id.as_str()).store()?;
                        due.remove((row.created_at.timestamp_millis(), publish_id.as_str()))
                            .store()?;
                    }
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Acquire one more reference for each listed blob.
    ///
    /// All increments commit together. If any id is claimed for deletion, the
    /// transaction rolls back. The call fails. A failure on any item leaves no
    /// reference acquired.
    #[instrument(skip(self, items), level = "debug", err)]
    pub(crate) async fn acquire_blob_refs(&self, items: &[(String, u64)]) -> S3Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let items = items.to_vec();
        let store = self.store.clone();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BLOBS).store()?;
                    for (id, size) in &items {
                        if store.fail_blob_insert(id) {
                            return Err(S3Error::internal_error(std::io::Error::other(
                                "injected blob insert failure",
                            )));
                        }

                        if let Some(mut row) = blob_row(&table, id).store()? {
                            row.refs += 1;
                            if row.refs <= 0 {
                                return Err(S3Error::internal_error(std::io::Error::other(
                                    format!(
                                        "blob {id} is claimed for deletion; the acquire was rolled back"
                                    ),
                                )));
                            }
                            let bytes = encode(&row).store()?;
                            table.insert(id.as_str(), bytes.as_slice()).store()?;
                        } else {
                            let row = BlobRow {
                                id: id.clone(),
                                size: Repository::checked_size(*size)?,
                                refs: 1,
                                created_at: chrono::Local::now().to_utc(),
                            };
                            let bytes = encode(&row).store()?;
                            table.insert(id.as_str(), bytes.as_slice()).store()?;
                        }
                    }
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Drop one reference for each listed blob id. Return the ids whose count
    /// reached zero.
    ///
    /// The rows of returned ids are kept. The caller must try the backend
    /// delete first. Then the caller must call
    /// [`Self::delete_released_blob_rows`] for removed ids.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub(crate) async fn release_blob_refs(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        self.inject(Fault::BlobRelease)?;
        let ids = ids.to_vec();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let released = release_blob_refs_in_txn(&txn, &ids)?;
                txn.commit().store()?;

                Ok(released)
            })
            .await
    }

    /// Claim released rows for backend deletion.
    ///
    /// The claim moves `refs` to [`BLOB_REF_DELETING`]. Only one caller can
    /// own the delete. An acquirer that meets the sentinel fails.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub(crate) async fn claim_released_blob_rows(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut distinct = ids.to_vec();
        distinct.sort_unstable();
        distinct.dedup();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let mut claimed = Vec::new();
                {
                    let mut table = txn.open_table(BLOBS).store()?;
                    for id in &distinct {
                        let Some(mut row) = blob_row(&table, id).store()? else {
                            continue;
                        };
                        if row.refs <= 0 && row.refs != BLOB_REF_DELETING {
                            row.refs = BLOB_REF_DELETING;
                            let bytes = encode(&row).store()?;
                            table.insert(id.as_str(), bytes.as_slice()).store()?;
                            claimed.push(id.clone());
                        }
                    }
                }
                txn.commit().store()?;
                claimed.sort_unstable();

                Ok(claimed)
            })
            .await
    }

    /// Put claimed rows back into the released state.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub(crate) async fn restore_released_blob_rows(&self, ids: &[String]) -> S3Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let ids = ids.to_vec();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BLOBS).store()?;
                    for id in &ids {
                        let Some(mut row) = blob_row(&table, id).store()? else {
                            continue;
                        };
                        if row.refs == BLOB_REF_DELETING {
                            row.refs = 0;
                            let bytes = encode(&row).store()?;
                            table.insert(id.as_str(), bytes.as_slice()).store()?;
                        }
                    }
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Current reference count for a blob id. Returns 0 if the row is absent.
    #[allow(dead_code)]
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn blob_ref_count(&self, id: &str) -> S3Result<i64> {
        let id = id.to_owned();
        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(BLOBS).store()?;
                let Some(row) = blob_row(&table, &id).store()? else {
                    return Ok(0);
                };

                Ok(row.refs)
            })
            .await
    }

    /// Reference counts for several blob ids in one query.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub(crate) async fn blob_ref_counts(&self, ids: &[String]) -> S3Result<HashMap<String, i64>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let ids = ids.to_vec();
        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(BLOBS).store()?;
                let mut counts = HashMap::new();
                for id in &ids {
                    let refs = blob_row(&table, id).store()?.map_or(0, |row| row.refs);
                    counts.insert(id.clone(), refs);
                }

                Ok(counts)
            })
            .await
    }

    /// Delete blob rows whose backend data the caller removed.
    ///
    /// The delete is guarded on `refs <= 0`. A reference acquired between the
    /// decision and this delete cannot lose the row.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub(crate) async fn delete_released_blob_rows(&self, ids: &[String]) -> S3Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let ids = ids.to_vec();
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BLOBS).store()?;
                    for id in &ids {
                        let Some(row) = blob_row(&table, id).store()? else {
                            continue;
                        };
                        if row.refs <= 0 {
                            table.remove(id.as_str()).store()?;
                        }
                    }
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Reconciliation caps. They keep a start-up pass bounded. The bound holds
    /// no matter how large the database is.
    const BLOB_SCAN_BATCH: u64 = 512;
    const BLOB_DELETE_CHUNK: usize = 256;
    const BLOB_MAX_CANDIDATES: u64 = 250_000;

    /// Drop blob rows older than `older_than`. The rows must be referenced
    /// by neither an object version nor an in-progress multipart upload.
    ///
    /// This is the only pass that reclaims a row left by an interrupted
    /// publish. Such a row is invisible to every list operation. Nothing else
    /// scans the blob table.
    ///
    /// The pass is bounded on every axis:
    ///  * Candidates are read in [`Self::BLOB_SCAN_BATCH`]-row keyset pages.
    ///    The candidate set is capped at [`Self::BLOB_MAX_CANDIDATES`] ids.
    ///  * Holders are streamed in the same page size in key order. Only one
    ///    page of `content` JSON is in memory at a time.
    ///  * Deletes are chunked. No transaction is wider than one page.
    ///
    /// No transaction spans the pass. Each page is its own short transaction.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn reconcile_orphan_blobs(
        &self,
        older_than: DateTime<Utc>,
    ) -> S3Result<usize> {
        let candidates = self.orphan_blob_candidates(older_than).await?;

        let Some(candidates) = candidates else {
            return Ok(0);
        };

        let mut referenced: HashSet<&str> = HashSet::new();
        self.stream_object_blob_ids(&candidates, &mut referenced)
            .await?;
        self.stream_multipart_blob_ids(&candidates, &mut referenced)
            .await?;

        let orphans: Vec<String> = candidates
            .iter()
            .filter(|id| !referenced.contains(id.as_str()))
            .cloned()
            .collect();

        if orphans.is_empty() {
            return Ok(0);
        }

        self.delete_orphan_blob_rows(&orphans, older_than).await?;

        Ok(orphans.len())
    }

    async fn delete_orphan_blob_rows(
        &self,
        ids: &[String],
        older_than: DateTime<Utc>,
    ) -> S3Result<()> {
        for chunk in ids.chunks(Self::BLOB_DELETE_CHUNK) {
            let chunk = chunk.to_vec();
            self.store
                .run(move |db| -> S3Result<()> {
                    let txn = db.begin_write().store()?;
                    {
                        let mut table = txn.open_table(BLOBS).store()?;
                        for id in &chunk {
                            let Some(row) = blob_row(&table, id).store()? else {
                                continue;
                            };
                            if row.created_at < older_than {
                                table.remove(id.as_str()).store()?;
                            }
                        }
                    }
                    txn.commit().store()?;

                    Ok(())
                })
                .await?;
        }

        Ok(())
    }

    /// Ids of blob rows old enough to be garbage. Returns `None` when the set
    /// would exceed the reconciliation cap.
    async fn orphan_blob_candidates(
        &self,
        older_than: DateTime<Utc>,
    ) -> S3Result<Option<HashSet<String>>> {
        let mut candidates: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;

        loop {
            let cursor_page = cursor.clone();
            let page = self
                .store
                .run(move |db| -> S3Result<Vec<(String, DateTime<Utc>)>> {
                    let txn = db.begin_read().store()?;
                    let table = txn.open_table(BLOBS).store()?;
                    let start: Bound<&str> = cursor_page
                        .as_deref()
                        .map_or(Bound::Unbounded, Bound::Excluded);
                    let mut page = Vec::new();

                    for entry in table.range::<&str>((start, Bound::Unbounded)).store()? {
                        let (key, value) = entry.store()?;
                        let id = key.value().to_owned();
                        let row: BlobRow = decode(value.value()).store()?;
                        page.push((id, row.created_at));
                        if page.len() as u64 == Self::BLOB_SCAN_BATCH {
                            break;
                        }
                    }

                    Ok(page)
                })
                .await?;

            let Some((last, _)) = page.last() else {
                break;
            };
            cursor = Some(last.clone());

            for (id, created_at) in page {
                if created_at < older_than {
                    candidates.insert(id);
                }
            }

            if candidates.len() as u64 > Self::BLOB_MAX_CANDIDATES {
                tracing::warn!(
                    candidates = candidates.len(),
                    cap = Self::BLOB_MAX_CANDIDATES,
                    error.type = "reconciliation_cap",
                    "skipped blob reconciliation: too many candidate blobs to check within the cap"
                );
                return Ok(None);
            }
        }

        Ok(Some(candidates))
    }

    /// Stream every object version. Collect the referenced ids that are among
    /// `candidates`.
    async fn stream_object_blob_ids<'a>(
        &self,
        candidates: &'a HashSet<String>,
        out: &mut HashSet<&'a str>,
    ) -> S3Result<()> {
        let mut cursor: Option<(String, String, String)> = None;

        loop {
            let cursor_page = cursor.clone();
            let page = self
                .store
                .run(
                    move |db| -> S3Result<Vec<(String, String, String, serde_json::Value)>> {
                        let txn = db.begin_read().store()?;
                        let table = txn.open_table(OBJECTS).store()?;
                        let start = cursor_page.as_ref().map_or(
                            Bound::Included(("", "", "")),
                            |(bucket, key, version)| {
                                Bound::Excluded((bucket.as_str(), key.as_str(), version.as_str()))
                            },
                        );
                        let mut page = Vec::new();

                        for entry in table.range((start, Bound::Unbounded)).store()? {
                            let (key, value) = entry.store()?;
                            let (bucket, key, version) = key.value();
                            let row: crate::storage::ObjectRow = decode(value.value()).store()?;
                            page.push((
                                bucket.to_owned(),
                                key.to_owned(),
                                version.to_owned(),
                                row.content,
                            ));
                            if page.len() as u64 == Self::BLOB_SCAN_BATCH {
                                break;
                            }
                        }

                        Ok(page)
                    },
                )
                .await?;

            let Some((bucket, key, version, _)) = page.last() else {
                break;
            };
            cursor = Some((bucket.clone(), key.clone(), version.clone()));

            for (_, _, _, content) in page {
                collect_referenced_ids(&content, candidates, out);
            }
        }

        Ok(())
    }

    /// Stream every in-progress multipart upload. Collect the referenced ids
    /// that are among `candidates`.
    async fn stream_multipart_blob_ids<'a>(
        &self,
        candidates: &'a HashSet<String>,
        out: &mut HashSet<&'a str>,
    ) -> S3Result<()> {
        let mut cursor: Option<(String, String, String)> = None;

        loop {
            let cursor_page = cursor.clone();
            let page = self
                .store
                .run(
                    move |db| -> S3Result<Vec<(String, String, String, serde_json::Value)>> {
                        let txn = db.begin_read().store()?;
                        let table = txn.open_table(MULTIPART).store()?;
                        let start = cursor_page.as_ref().map_or(
                            Bound::Included(("", "", "")),
                            |(bucket, key, upload)| {
                                Bound::Excluded((bucket.as_str(), key.as_str(), upload.as_str()))
                            },
                        );
                        let mut page = Vec::new();

                        for entry in table.range((start, Bound::Unbounded)).store()? {
                            let (key, value) = entry.store()?;
                            let (bucket, key, upload) = key.value();
                            let row: crate::storage::MultipartUploadRow =
                                decode(value.value()).store()?;
                            page.push((
                                bucket.to_owned(),
                                key.to_owned(),
                                upload.to_owned(),
                                row.content,
                            ));
                            if page.len() as u64 == Self::BLOB_SCAN_BATCH {
                                break;
                            }
                        }

                        Ok(page)
                    },
                )
                .await?;

            let Some((bucket, key, upload, _)) = page.last() else {
                break;
            };
            cursor = Some((bucket.clone(), key.clone(), upload.clone()));

            for (_, _, _, content) in page {
                collect_referenced_ids(&content, candidates, out);
            }
        }

        Ok(())
    }
}

pub(super) fn release_blob_refs_in_txn(
    txn: &WriteTransaction,
    ids: &[String],
) -> S3Result<Vec<String>> {
    let mut counts: HashMap<&str, i64> = HashMap::new();
    for id in ids {
        *counts.entry(id.as_str()).or_default() += 1;
    }

    let mut distinct: Vec<&str> = counts.keys().copied().collect();
    distinct.sort_unstable();

    let mut released = Vec::new();
    {
        let mut table = txn.open_table(BLOBS).store()?;
        for id in distinct {
            let n = counts[id];
            let Some(mut row) = blob_row(&table, id).store()? else {
                continue;
            };
            if row.refs <= 0 {
                continue;
            }
            row.refs = if row.refs <= n { 0 } else { row.refs - n };
            let bytes = encode(&row).store()?;
            table.insert(id, bytes.as_slice()).store()?;
            if row.refs <= 0 {
                released.push(id.to_owned());
            }
        }
    }

    released.sort_unstable();
    Ok(released)
}

/// Collect the ids a `content` document points at. Keep only candidates for
/// removal.
///
/// The two shapes differ. An object uses `{"item": [{"id": ...}]}`. An
/// upload uses `{"<part>": {"metadata_items": [{"id": ...}]}}`. The walk
/// takes every `id` string it finds. It filters that string against the
/// candidate set. The walk borrows, so streaming a page allocates nothing
/// beyond the set.
fn collect_referenced_ids<'a>(
    content: &serde_json::Value,
    candidates: &'a HashSet<String>,
    out: &mut HashSet<&'a str>,
) {
    match content {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                if key == "id"
                    && let serde_json::Value::String(id) = value
                    && let Some(canonical) = candidates.get(id.as_str())
                {
                    out.insert(canonical.as_str());
                    continue;
                }
                collect_referenced_ids(value, candidates, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_referenced_ids(item, candidates, out);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;
    use crate::s3::repo::{MultipartUploadStateUpdate, ObjectWrite, PutCondition};
    use crate::storage::Store;

    async fn repo() -> Repository {
        let store = Store::in_memory().expect("store");
        Repository::init(store).await.expect("init")
    }

    /// A blob row as old as the reconciler's threshold wants it.
    fn seed_old_blob(repo: &Repository, id: &str) {
        seed_blob(
            repo,
            id,
            chrono::Local::now().to_utc() - chrono::Duration::hours(2),
        );
    }

    fn seed_blob(repo: &Repository, id: &str, created_at: DateTime<Utc>) {
        repo.store.seed_blob(&BlobRow {
            id: id.to_string(),
            size: 1,
            refs: 1,
            created_at,
        });
    }

    fn object_content(blob_id: &str) -> serde_json::Value {
        serde_json::json!({"item": [{"id": blob_id, "offset": 0, "size": 1}]})
    }

    fn upload_content(blob_id: &str) -> serde_json::Value {
        serde_json::json!({
            "1": {"hash": "d", "metadata_items": [{"id": blob_id, "offset": 0, "size": 1}]}
        })
    }

    async fn put_object(repo: &Repository, key: &str, blob_id: &str) {
        repo.cas_put_object(
            "b".into(),
            key.to_string(),
            ObjectWrite {
                size: 1,
                content_type: None,
                etag: Some("e1".into()),
                content: object_content(blob_id),
                user_metadata: serde_json::json!({}),
                checksums: serde_json::json!({}),
                tags: serde_json::json!([]),
            },
            PutCondition::None,
        )
        .await
        .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
    }

    fn blob_ids(repo: &Repository) -> Vec<String> {
        let mut ids: Vec<String> = repo
            .store
            .all_blobs()
            .into_iter()
            .map(|row| row.id)
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn reconciler_removes_unreferenced_rows_only() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        seed_old_blob(&repo, "orphan");
        seed_old_blob(&repo, "held-by-object");
        seed_old_blob(&repo, "held-by-upload");

        put_object(&repo, "k", "held-by-object").await;
        repo.upsert_multipart_upload_state(MultipartUploadStateUpdate {
            bucket: "b".into(),
            key: "k".into(),
            upload_id: "u1".into(),
            content_type: None,
            user_metadata: serde_json::json!({}),
            tags: serde_json::json!([]),
            content: upload_content("held-by-upload"),
        })
        .await
        .expect("in-progress upload");

        let removed = repo
            .reconcile_orphan_blobs(chrono::Local::now().to_utc())
            .await
            .expect("reconcile");

        assert_eq!(removed, 1, "only the unreferenced row may go");
        assert_eq!(
            blob_ids(&repo),
            ["held-by-object", "held-by-upload"],
            "a blob held by an object version or an in-progress upload must survive"
        );
    }

    #[tokio::test]
    async fn reconciler_spares_rows_younger_than_the_threshold() {
        let repo = repo().await;

        repo.register_new_blob("fresh".into(), 1)
            .await
            .expect("register");

        let removed = repo
            .reconcile_orphan_blobs(chrono::Local::now().to_utc() - chrono::Duration::hours(1))
            .await
            .expect("reconcile");

        assert_eq!(removed, 0, "a young row may still be an in-flight write");
        assert_eq!(blob_ids(&repo), ["fresh"]);
    }

    #[tokio::test]
    async fn reconciler_walks_past_a_batch_boundary() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let total = Repository::BLOB_SCAN_BATCH as usize + Repository::BLOB_DELETE_CHUNK + 3;
        for i in 0..total {
            seed_old_blob(&repo, &format!("blob-{i:04}"));
        }
        seed_old_blob(&repo, "kept");
        put_object(&repo, "k", "kept").await;

        let removed = repo
            .reconcile_orphan_blobs(chrono::Local::now().to_utc())
            .await
            .expect("reconcile");

        assert_eq!(removed, total, "every unreferenced row must go");
        assert_eq!(blob_ids(&repo), ["kept"]);
    }

    #[tokio::test]
    async fn release_keeps_rows_until_the_backend_delete_succeeds() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");
        repo.register_new_blob("b2".into(), 1)
            .await
            .expect("register");

        let zero = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("release");
        assert_eq!(zero, ["b1"]);
        assert_eq!(blob_ids(&repo), ["b1", "b2"]);

        repo.delete_released_blob_rows(&["b1".to_string()])
            .await
            .expect("delete rows");
        assert_eq!(blob_ids(&repo), ["b2"]);

        repo.delete_released_blob_rows(&["b2".to_string()])
            .await
            .expect("no-op delete");
        assert_eq!(blob_ids(&repo), ["b2"]);
    }

    #[tokio::test]
    async fn release_counts_duplicate_ids_and_clamps_at_zero() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");

        let ids = vec!["b1".to_string(); 3];
        let released = repo.release_blob_refs(&ids).await.expect("release");
        assert_eq!(released, ["b1"]);
        assert_eq!(
            repo.blob_ref_count("b1").await.expect("count"),
            0,
            "three releases against one reference must not go negative"
        );

        let again = repo.release_blob_refs(&ids).await.expect("release again");
        assert!(again.is_empty(), "a zero row is not released twice");
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 0);
    }

    #[tokio::test]
    async fn release_reports_only_the_call_that_reaches_zero() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");
        repo.store.set_blob_refs("b1", 2);

        let first = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("first release");
        assert!(first.is_empty(), "one of two references still remains");
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 1);

        let second = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("second release");
        assert_eq!(second, ["b1"], "only the last release reports the blob");
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 0);

        let third = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("third release");
        assert!(third.is_empty(), "an already released row is not reported");
    }

    #[tokio::test]
    async fn a_reacquired_blob_is_not_claimed_for_deletion() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");

        let released = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("release");
        assert_eq!(released, ["b1"]);

        repo.acquire_blob_refs(&[("b1".to_string(), 1)])
            .await
            .expect("re-acquire");

        let claimed = repo
            .claim_released_blob_rows(&["b1".to_string()])
            .await
            .expect("claim");
        assert!(
            claimed.is_empty(),
            "a live reference must keep the row out of the delete claim"
        );
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_claimed_blob_cannot_be_acquired_until_it_is_restored() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");
        repo.release_blob_refs(&["b1".to_string()])
            .await
            .expect("release");

        let claimed = repo
            .claim_released_blob_rows(&["b1".to_string()])
            .await
            .expect("claim");
        assert_eq!(claimed, ["b1"]);
        assert_eq!(
            repo.blob_ref_count("b1").await.expect("count"),
            BLOB_REF_DELETING
        );

        let again = repo
            .claim_released_blob_rows(&["b1".to_string()])
            .await
            .expect("claim again");
        assert!(again.is_empty(), "one caller owns the delete");

        let err = repo
            .acquire_blob_refs(&[("b1".to_string(), 1)])
            .await
            .expect_err("an acquire must not resurrect a claimed blob");
        let cause = err.source().expect("the acquire cause is kept").to_string();
        assert!(
            cause.contains("claimed for deletion"),
            "the error must name the cause: {cause}"
        );
        assert_eq!(
            repo.blob_ref_count("b1").await.expect("count"),
            BLOB_REF_DELETING,
            "a failed acquire must roll back all of its increments"
        );

        repo.restore_released_blob_rows(&["b1".to_string()])
            .await
            .expect("restore");
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 0);

        repo.acquire_blob_refs(&[("b1".to_string(), 1)])
            .await
            .expect("acquire after restore");
        assert_eq!(repo.blob_ref_count("b1").await.expect("count"), 1);
    }

    #[tokio::test]
    async fn a_claim_takes_legacy_negative_rows_and_leaves_live_rows() {
        let repo = repo().await;
        repo.register_new_blob("live".into(), 1)
            .await
            .expect("register");
        repo.register_new_blob("legacy".into(), 1)
            .await
            .expect("register");
        repo.store.set_blob_refs("legacy", -3);

        let claimed = repo
            .claim_released_blob_rows(&["live".to_string(), "legacy".to_string()])
            .await
            .expect("claim");

        assert_eq!(
            claimed,
            ["legacy"],
            "a negative row from an old bug must still become collectable"
        );
        assert_eq!(repo.blob_ref_count("live").await.expect("count"), 1);
        assert_eq!(
            repo.blob_ref_count("legacy").await.expect("count"),
            BLOB_REF_DELETING
        );
    }

    /// A failure on one item rolls back references that earlier items acquired.
    #[tokio::test]
    async fn acquire_is_all_or_nothing_when_an_item_fails() {
        let repo = repo().await;
        repo.store.fail_blob_insert_for("bad");

        let result = repo
            .acquire_blob_refs(&[("good".to_string(), 1), ("bad".to_string(), 1)])
            .await;
        assert!(result.is_err(), "the failing item must fail the call");
        assert!(
            blob_ids(&repo).is_empty(),
            "no item may be acquired when a later item fails"
        );
    }
}
