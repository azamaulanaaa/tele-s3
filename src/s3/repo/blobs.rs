use std::collections::HashSet;

use s3s::{S3Error, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, ExprTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Set, TransactionTrait,
};
use tracing::instrument;

use super::Repository;
use super::entity;

/// Reference count that marks a row as claimed for backend deletion.
///
/// The value is below any count an old underflow can produce. A row left
/// negative by an old bug still reads as released, not as claimed.
const BLOB_REF_DELETING: i64 = i64::MIN;

impl Repository {
    /// Insert a blob row owned by a single reference.
    ///
    /// Takes any connection, so the row can be written on its own
    /// (`&self.db`) or inside a caller's transaction (see
    /// `publish_object_with_new_blob` in `super::objects`).
    pub(super) async fn insert_blob<C: ConnectionTrait>(
        conn: &C,
        id: String,
        size: u64,
    ) -> S3Result<()> {
        let active_model = entity::blob::ActiveModel {
            id: Set(id),
            size: Set(Self::checked_size(size)?),
            refs: Set(1),
            created_at: Set(chrono::Local::now().to_utc()),
        };

        entity::blob::Entity::insert(active_model)
            .exec(conn)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    /// Register a freshly created backend blob with a single reference.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn register_new_blob(&self, id: String, size: u64) -> S3Result<()> {
        Self::insert_blob(&self.db, id, size).await
    }

    /// Record the intent to publish an object, *before* the backend is called.
    ///
    /// The write is the caller's own identifier (`publish_id`) so that an
    /// object published without its answer coming back can still be found by
    /// that identifier; this row is the durable record of that identifier and
    /// of what it was written for. See
    /// [`entity::publish_intent`](super::entity::publish_intent) for the
    /// state machine.
    ///
    /// Inserted up front rather than after the call so a failure — including a
    /// failure with no answer at all — leaves the intent on record. A caller
    /// that cannot record the intent must not call the backend: an
    /// unrecorded publish is unrecoverable.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn record_publish_intent(
        &self,
        publish_id: String,
        bucket_id: String,
        object_id: String,
        size: u64,
    ) -> S3Result<()> {
        let active_model = entity::publish_intent::ActiveModel {
            publish_id: Set(publish_id),
            bucket_id: Set(bucket_id),
            object_id: Set(object_id),
            size: Set(Self::checked_size(size)?),
            blob_id: Set(None),
            created_at: Set(chrono::Local::now().to_utc()),
            resolved_at: Set(None),
        };

        entity::publish_intent::Entity::insert(active_model)
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    /// Mark a recorded intent resolved now that the backend returned a key.
    ///
    /// Only ever called on the success path. A failed write leaves the row
    /// pending on purpose — that pending row is the only record an
    /// unpublished-but-published-anyway object can be found by.
    ///
    /// Test-only: every production success path resolves inside the publish
    /// transaction, through [`Self::resolve_publish_intent_txn`].
    #[cfg(test)]
    pub async fn resolve_publish_intent(&self, publish_id: &str, blob_id: String) -> S3Result<()> {
        Self::resolve_publish_intent_txn(&self.db, publish_id, blob_id).await
    }

    /// Mark an intent resolved on any connection.
    ///
    /// The metadata publish passes its own transaction here. The object row,
    /// the blob row and the resolved intent then commit together, so a
    /// committed object can never leave its intent pending for the reclaim
    /// pass to delete its live bytes.
    ///
    /// A missing or already-resolved row is an error. At this point the write
    /// has already produced a backend blob, so continuing would create the
    /// unrecorded publish that the intent exists to prevent.
    #[instrument(skip(conn), level = "debug", err)]
    pub(super) async fn resolve_publish_intent_txn<C: ConnectionTrait>(
        conn: &C,
        publish_id: &str,
        blob_id: String,
    ) -> S3Result<()> {
        let now = chrono::Local::now().to_utc();

        let result = entity::publish_intent::Entity::update_many()
            .col_expr(
                entity::publish_intent::Column::BlobId,
                sea_orm::sea_query::Expr::value(blob_id),
            )
            .col_expr(
                entity::publish_intent::Column::ResolvedAt,
                sea_orm::sea_query::Expr::value(now),
            )
            .filter(entity::publish_intent::Column::PublishId.eq(publish_id))
            .filter(entity::publish_intent::Column::ResolvedAt.is_null())
            .exec(conn)
            .await
            .map_err(S3Error::internal_error)?;

        if result.rows_affected != 1 {
            return Err(S3Error::internal_error(std::io::Error::other(
                "publish intent was not pending",
            )));
        }

        Ok(())
    }

    /// Pending publish intents older than `older_than`, oldest first, capped
    /// at `limit` rows.
    ///
    /// The age filter is the whole safety rule of the pass: an intent younger
    /// than the threshold may belong to a write that is still in flight, and
    /// acting on it would look for a publish that has not happened yet. It is
    /// applied here rather than in the caller so the query and the rule cannot
    /// drift apart.
    ///
    /// Bounded by `limit` and ordered by the primary key, so one page is
    /// enough for a pass and no statement — and therefore no write lock — is
    /// ever wider than that page.
    pub async fn pending_publish_intents(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
        limit: u64,
    ) -> S3Result<Vec<entity::publish_intent::Model>> {
        entity::publish_intent::Entity::find()
            .filter(entity::publish_intent::Column::ResolvedAt.is_null())
            .filter(entity::publish_intent::Column::CreatedAt.lt(older_than))
            .order_by_asc(entity::publish_intent::Column::PublishId)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)
    }

    /// Drop the recorded intents whose publishes are settled.
    ///
    /// Guarded on `resolved_at IS NULL` so a write that resolved between the
    /// selection and this statement keeps its row: a resolved row is the only
    /// record of which key a confirmed publish produced, and the reconciler
    /// has no business removing one.
    pub async fn clear_publish_intents(&self, publish_ids: &[String]) -> S3Result<()> {
        if publish_ids.is_empty() {
            return Ok(());
        }

        entity::publish_intent::Entity::delete_many()
            .filter(entity::publish_intent::Column::PublishId.is_in(publish_ids.to_vec()))
            .filter(entity::publish_intent::Column::ResolvedAt.is_null())
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    /// Acquire one additional reference for each listed blob.
    ///
    /// Each upsert is atomic (`INSERT ... ON CONFLICT DO UPDATE SET refs =
    /// refs + 1`), so concurrent acquirers cannot lose increments the way a
    /// check-then-insert can. A previously unknown blob is seeded with a
    /// single reference owned by the caller acquiring it.
    ///
    /// All increments commit together. If any id is claimed for deletion,
    /// the transaction rolls back and the call fails: the backend bytes may
    /// already be gone, so a reference must never be created for that id.
    /// A failure on any item therefore leaves no reference acquired by this
    /// call, so the caller has nothing to release after it returns an error.
    #[instrument(skip(self, items), level = "debug", err)]
    pub async fn acquire_blob_refs(&self, items: &[(String, u64)]) -> S3Result<()> {
        use sea_orm::{ConnectionTrait, DbBackend, Statement};

        if items.is_empty() {
            return Ok(());
        }

        let backend = self.db.get_database_backend();
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result: S3Result<()> = async {
            for (id, size) in items {
                let size32 = Self::checked_size(*size)?;
                let now = chrono::Local::now().to_utc();
                let values = [
                    sea_orm::Value::from(id.clone()),
                    sea_orm::Value::from(size32),
                    sea_orm::Value::from(now),
                ];
                let sql = match backend {
                    DbBackend::MySql => "INSERT INTO `s3_blob` (`id`, `size`, `refs`, `created_at`) VALUES (?, ?, 1, ?) ON DUPLICATE KEY UPDATE `refs` = `refs` + 1".to_string(),
                    DbBackend::Postgres => "INSERT INTO \"s3_blob\" (\"id\", \"size\", \"refs\", \"created_at\") VALUES ($1, $2, 1, $3) ON CONFLICT (\"id\") DO UPDATE SET \"refs\" = \"s3_blob\".\"refs\" + 1".to_string(),
                    _ => "INSERT INTO \"s3_blob\" (\"id\", \"size\", \"refs\", \"created_at\") VALUES (?, ?, 1, ?) ON CONFLICT (\"id\") DO UPDATE SET \"refs\" = \"s3_blob\".\"refs\" + 1".to_string(),
                };

                txn.execute_raw(Statement::from_sql_and_values(backend, sql, values))
                    .await
                    .map_err(S3Error::internal_error)?;

                let after = entity::blob::Entity::find_by_id(id.clone())
                    .one(&txn)
                    .await
                    .map_err(S3Error::internal_error)?;

                let acquired = matches!(after, Some(model) if model.refs > 0);
                if !acquired {
                    return Err(S3Error::internal_error(std::io::Error::other(format!(
                        "blob {id} is claimed for deletion; the acquire was rolled back"
                    ))));
                }
            }

            Ok(())
        }
        .await;

        match result {
            Ok(()) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(())
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Drop one reference for each listed blob id and return the ids whose
    /// count reached zero.
    /// Drop one reference for each listed blob id and return the ids whose
    /// count reached zero.
    ///
    /// Each id is decremented by one atomic statement. The update respects a
    /// concurrent increment, clamps the count at zero, and only one caller
    /// can report the id that reached zero.
    ///
    /// The rows of the returned ids are deliberately *kept*: the caller must
    /// attempt the backend delete first and only then call
    /// [`Self::delete_released_blob_rows`] for the ids it actually removed.
    /// Dropping the bookkeeping before the bytes are gone would leave a
    /// blob that exists only in the backend, with no record anywhere that it
    /// is unreferenced, and nothing that ever retries it.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn release_blob_refs(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;
        let result = Self::release_blob_refs_on(&txn, ids).await;

        match result {
            Ok(released) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(released)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Drop one reference for each listed blob id on `conn` and return the
    /// ids whose count reached zero.
    ///
    /// The caller owns the transaction. That lets a larger state change (such
    /// as deleting a multipart upload) commit together with the reference
    /// release, so a failure cannot separate the two.
    ///
    /// Each id is decremented by one atomic statement. The update respects a
    /// concurrent increment, clamps the count at zero, and only one caller
    /// can report the id that reached zero.
    pub(super) async fn release_blob_refs_on<C: ConnectionTrait>(
        conn: &C,
        ids: &[String],
    ) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // The same blob may back several slices of one object (e.g.
        // repeated UploadPartCopy ranges); each slice holds its own
        // reference, so count occurrences instead of decrementing once.
        let mut counts: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for id in ids {
            *counts.entry(id.as_str()).or_default() += 1;
        }

        let mut distinct: Vec<&str> = counts.keys().copied().collect();
        distinct.sort_unstable();

        let mut released: Vec<String> = Vec::new();

        for id in distinct {
            let n = counts[id];

            // One statement subtracts and clamps. A row that is already
            // zero or negative is left alone, so a repeated release can
            // never drive the count down again.
            let updated = entity::blob::Entity::update_many()
                .col_expr(
                    entity::blob::Column::Refs,
                    Expr::case(
                        Expr::col(entity::blob::Column::Refs).lte(n),
                        Expr::value(0i64),
                    )
                    .finally(Expr::col(entity::blob::Column::Refs).sub(n))
                    .into(),
                )
                .filter(entity::blob::Column::Id.eq(id))
                .filter(entity::blob::Column::Refs.gt(0))
                .exec(conn)
                .await
                .map_err(S3Error::internal_error)?;

            if updated.rows_affected == 0 {
                // Absent, already released, or claimed for deletion.
                continue;
            }

            // The update holds the row until the caller commits, so this
            // read sees the value this call produced and no later writer.
            let after = entity::blob::Entity::find_by_id(id.to_string())
                .one(conn)
                .await
                .map_err(S3Error::internal_error)?;

            if after.is_none_or(|model| model.refs <= 0) {
                released.push(id.to_string());
            }
        }

        released.sort_unstable();
        Ok(released)
    }

    /// Claim released rows for backend deletion.
    ///
    /// The claim moves `refs` to [`BLOB_REF_DELETING`], so only one caller
    /// can own the delete. An acquirer that meets the sentinel fails and
    /// rolls back instead of creating a reference to deleted bytes.
    ///
    /// Returns the ids this call claimed. A row that another caller already
    /// claimed is not returned.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn claim_released_blob_rows(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result: S3Result<Vec<String>> = async {
            let mut distinct: Vec<&str> = ids.iter().map(String::as_str).collect();
            distinct.sort_unstable();
            distinct.dedup();

            let mut claimed: Vec<String> = Vec::new();

            for id in distinct {
                let updated = entity::blob::Entity::update_many()
                    .col_expr(entity::blob::Column::Refs, Expr::value(BLOB_REF_DELETING))
                    .filter(entity::blob::Column::Id.eq(id))
                    .filter(entity::blob::Column::Refs.lte(0))
                    .filter(entity::blob::Column::Refs.ne(BLOB_REF_DELETING))
                    .exec(&txn)
                    .await
                    .map_err(S3Error::internal_error)?;

                if updated.rows_affected > 0 {
                    claimed.push(id.to_string());
                }
            }

            claimed.sort_unstable();
            Ok(claimed)
        }
        .await;

        match result {
            Ok(claimed) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(claimed)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Put claimed rows back into the released state.
    ///
    /// Call this when the backend delete fails. A later release can then try
    /// the delete again.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn restore_released_blob_rows(&self, ids: &[String]) -> S3Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        for chunk in ids.chunks(Self::BLOB_DELETE_CHUNK) {
            entity::blob::Entity::update_many()
                .col_expr(entity::blob::Column::Refs, Expr::value(0i64))
                .filter(entity::blob::Column::Id.is_in(chunk.to_vec()))
                .filter(entity::blob::Column::Refs.eq(BLOB_REF_DELETING))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;
        }

        Ok(())
    }

    /// Current reference count for a blob id (0 if the row is absent).
    #[instrument(skip(self), level = "debug", err)]
    pub async fn blob_ref_count(&self, id: &str) -> S3Result<i64> {
        let model = entity::blob::Entity::find_by_id(id.to_string())
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?;
        Ok(model.map(|m| m.refs).unwrap_or(0))
    }

    /// Delete blob rows whose backend data the caller has already removed.
    ///
    /// Guarded on `refs <= 0` so a reference acquired between the decision
    /// and this statement can never take the row away from a live holder.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn delete_released_blob_rows(&self, ids: &[String]) -> S3Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        for chunk in ids.chunks(Self::BLOB_DELETE_CHUNK) {
            entity::blob::Entity::delete_many()
                .filter(entity::blob::Column::Id.is_in(chunk.to_vec()))
                .filter(entity::blob::Column::Refs.lte(0))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;
        }

        Ok(())
    }

    /// Reconciliation caps, chosen so a start-up pass is bounded no matter
    /// how large the database already is.
    const BLOB_SCAN_BATCH: u64 = 512;
    const BLOB_DELETE_CHUNK: usize = 256;
    const BLOB_MAX_CANDIDATES: u64 = 250_000;

    /// Drop blob rows that are older than `older_than` and referenced by
    /// neither an object version nor an in-progress multipart upload.
    ///
    /// This is the only thing that ever reclaims the row a publish
    /// interrupted between "record the blob" and "publish the object"
    /// leaves behind: such a row is invisible to every list operation and
    /// nothing else scans `s3_blob`.
    ///
    /// Bounded on every axis:
    ///  * candidates are read in [`Self::BLOB_SCAN_BATCH`]-row keyset
    ///    pages, and the candidate set is capped at
    ///    [`Self::BLOB_MAX_CANDIDATES`] ids — over the cap the pass is
    ///    abandoned with a warning rather than growing without limit;
    ///  * holders are streamed in the same page size over the primary-key
    ///    order, so only one page of `content` JSON is materialised at a
    ///    time and no more than `candidates` ids are held in memory;
    ///  * deletes are chunked, so no statement — and therefore no write
    ///    lock — is wider than one page.
    ///
    /// The holder pass is deliberately *not* filtered by age: a copy
    /// published today can still point at a two-year-old blob, so every
    /// version has to be visited. It is a single linear pass reading the key
    /// columns plus `content`, and it is the reason the pass is bounded by
    /// memory rather than by `LIMIT`.
    ///
    /// No transaction spans the pass: every page is its own short implicit
    /// transaction, so the reconciler never blocks writers for the length
    /// of a full scan.
    ///
    /// Returns the number of rows removed. Blob rows are metadata only —
    /// reclaiming the backend bytes of the orphans is the job of the
    /// release path (and, for rows whose backend delete failed, of a
    /// backend-aware sweeper this pass deliberately does not attempt).
    #[instrument(skip(self), level = "debug", err)]
    pub async fn reconcile_orphan_blobs(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> S3Result<usize> {
        let candidates = self.orphan_blob_candidates(older_than).await?;

        let Some(candidates) = candidates else {
            return Ok(0);
        };

        // Ids of candidates that some holder still points at. Only these and
        // `candidates` are ever held, both bounded by the cap above.
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

    /// Delete the rows the reconciler proved unreferenced.
    ///
    /// Guarded on `created_at < older_than` so the age rule of the pass that
    /// selected them cannot be widened by a concurrent write, and chunked so
    /// a single statement never spans more than one page of ids.
    async fn delete_orphan_blob_rows(
        &self,
        ids: &[String],
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> S3Result<()> {
        for chunk in ids.chunks(Self::BLOB_DELETE_CHUNK) {
            entity::blob::Entity::delete_many()
                .filter(entity::blob::Column::Id.is_in(chunk.to_vec()))
                .filter(entity::blob::Column::CreatedAt.lt(older_than))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;
        }

        Ok(())
    }

    /// Ids of blob rows old enough to be garbage, or `None` when the set
    /// would exceed the reconciliation cap.
    async fn orphan_blob_candidates(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
    ) -> S3Result<Option<HashSet<String>>> {
        let mut candidates: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;

        loop {
            let page = entity::blob::Entity::find()
                .select_only()
                .column(entity::blob::Column::Id)
                .filter(entity::blob::Column::CreatedAt.lt(older_than))
                .apply_if(cursor.as_ref(), |query, c| {
                    query.filter(entity::blob::Column::Id.gt(c))
                })
                .order_by_asc(entity::blob::Column::Id)
                .limit(Self::BLOB_SCAN_BATCH)
                .into_tuple::<String>()
                .all(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let last = match page.last() {
                Some(last) => last.clone(),
                None => break,
            };
            cursor = Some(last.clone());

            candidates.extend(page);

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

    /// Stream every object version, collecting the referenced ids that are
    /// among `candidates`.
    async fn stream_object_blob_ids<'a>(
        &self,
        candidates: &'a HashSet<String>,
        out: &mut HashSet<&'a str>,
    ) -> S3Result<()> {
        let mut cursor: Option<(String, String, String)> = None;

        loop {
            let page = entity::object::Entity::find()
                .select_only()
                .columns([
                    entity::object::Column::BucketId,
                    entity::object::Column::Id,
                    entity::object::Column::VersionId,
                    entity::object::Column::Content,
                ])
                .apply_if(cursor.as_ref(), |query, (bucket, id, version)| {
                    // Keyset walk of the `(bucket_id, id, version_id)` primary
                    // key: "strictly after" the last row, never a rescan.
                    query.filter(
                        Condition::any()
                            .add(entity::object::Column::BucketId.gt(bucket.clone()))
                            .add(
                                Condition::all()
                                    .add(entity::object::Column::BucketId.eq(bucket.clone()))
                                    .add(entity::object::Column::Id.gt(id.clone())),
                            )
                            .add(
                                Condition::all()
                                    .add(entity::object::Column::BucketId.eq(bucket.clone()))
                                    .add(entity::object::Column::Id.eq(id.clone()))
                                    .add(entity::object::Column::VersionId.gt(version.clone())),
                            ),
                    )
                })
                .order_by_asc(entity::object::Column::BucketId)
                .order_by_asc(entity::object::Column::Id)
                .order_by_asc(entity::object::Column::VersionId)
                .limit(Self::BLOB_SCAN_BATCH)
                .into_tuple::<(String, String, String, serde_json::Value)>()
                .all(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let last = match page.last() {
                Some(last) => (last.0.clone(), last.1.clone(), last.2.clone()),
                None => break,
            };
            cursor = Some(last);

            for (_, _, _, content) in page {
                collect_referenced_ids(&content, candidates, out);
            }
        }

        Ok(())
    }

    /// Stream every in-progress multipart upload, collecting the referenced
    /// ids that are among `candidates`.
    async fn stream_multipart_blob_ids<'a>(
        &self,
        candidates: &'a HashSet<String>,
        out: &mut HashSet<&'a str>,
    ) -> S3Result<()> {
        let mut cursor: Option<(String, String, String)> = None;

        loop {
            let page = entity::multipart_upload_state::Entity::find()
                .select_only()
                .columns([
                    entity::multipart_upload_state::Column::BucketId,
                    entity::multipart_upload_state::Column::ObjectId,
                    entity::multipart_upload_state::Column::UploadId,
                    entity::multipart_upload_state::Column::Content,
                ])
                .apply_if(cursor.as_ref(), |query, (bucket, object_id, upload_id)| {
                    query.filter(
                        Condition::any()
                            .add(
                                entity::multipart_upload_state::Column::BucketId.gt(bucket.clone()),
                            )
                            .add(
                                Condition::all()
                                    .add(
                                        entity::multipart_upload_state::Column::BucketId
                                            .eq(bucket.clone()),
                                    )
                                    .add(
                                        entity::multipart_upload_state::Column::ObjectId
                                            .gt(object_id.clone()),
                                    ),
                            )
                            .add(
                                Condition::all()
                                    .add(
                                        entity::multipart_upload_state::Column::BucketId
                                            .eq(bucket.clone()),
                                    )
                                    .add(
                                        entity::multipart_upload_state::Column::ObjectId
                                            .eq(object_id.clone()),
                                    )
                                    .add(
                                        entity::multipart_upload_state::Column::UploadId
                                            .gt(upload_id.clone()),
                                    ),
                            ),
                    )
                })
                .order_by_asc(entity::multipart_upload_state::Column::BucketId)
                .order_by_asc(entity::multipart_upload_state::Column::ObjectId)
                .order_by_asc(entity::multipart_upload_state::Column::UploadId)
                .limit(Self::BLOB_SCAN_BATCH)
                .into_tuple::<(String, String, String, serde_json::Value)>()
                .all(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let last = match page.last() {
                Some(last) => (last.0.clone(), last.1.clone(), last.2.clone()),
                None => break,
            };
            cursor = Some(last);

            for (_, _, _, content) in page {
                collect_referenced_ids(&content, candidates, out);
            }
        }

        Ok(())
    }
}

/// Collect the ids a `content` document points at that are candidates for
/// removal.
///
/// The two shapes differ (`{"item": [{"id": ...}]}` for an object,
/// `{"<part>": {"metadata_items": [{"id": ...}]}}` for an upload), and a
/// `LIKE '%'||id||'%'` predicate cannot express membership without
/// scanning every candidate per row, so the walk takes every `id` string it
/// finds and filters it against the candidate set. Borrowed, so streaming a
/// page of rows allocates nothing beyond the set.
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
                    // The canonical string is borrowed from `candidates`,
                    // so nothing in the walk outlives that set.
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
    use super::*;
    use crate::s3::repo::{MultipartUploadStateUpdate, ObjectWrite, PutCondition};

    async fn repo() -> Repository {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        Repository::init(db).await.expect("init")
    }

    /// A blob row as old as the reconciler's threshold wants it.
    async fn seed_old_blob(repo: &Repository, id: &str) {
        seed_blob(
            repo,
            id,
            chrono::Local::now().to_utc() - chrono::Duration::hours(2),
        )
        .await;
    }

    async fn seed_blob(repo: &Repository, id: &str, created_at: chrono::DateTime<chrono::Utc>) {
        entity::blob::Entity::insert(entity::blob::ActiveModel {
            id: Set(id.to_string()),
            size: Set(1),
            refs: Set(1),
            created_at: Set(created_at),
        })
        .exec(&repo.db)
        .await
        .unwrap_or_else(|e| panic!("seed blob {id}: {e:?}"));
    }

    async fn set_blob_refs(repo: &Repository, id: &str, refs: i64) {
        entity::blob::Entity::update_many()
            .col_expr(entity::blob::Column::Refs, Expr::value(refs))
            .filter(entity::blob::Column::Id.eq(id))
            .exec(&repo.db)
            .await
            .unwrap_or_else(|e| panic!("set refs for {id}: {e:?}"));
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

    async fn blob_ids(repo: &Repository) -> Vec<String> {
        let mut ids: Vec<String> = entity::blob::Entity::find()
            .all(&repo.db)
            .await
            .expect("load blob rows")
            .into_iter()
            .map(|m| m.id)
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

        // The shape a publish interrupted between its two metadata steps
        // leaves behind: a row nothing points at.
        seed_old_blob(&repo, "orphan").await;
        seed_old_blob(&repo, "held-by-object").await;
        seed_old_blob(&repo, "held-by-upload").await;

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
            blob_ids(&repo).await,
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
            .reconcile_orphan_blobs(
                (chrono::Local::now().to_utc() - chrono::Duration::hours(1)).to_owned(),
            )
            .await
            .expect("reconcile");

        assert_eq!(removed, 0, "a young row may still be an in-flight write");
        assert_eq!(blob_ids(&repo).await, ["fresh"]);
    }

    #[tokio::test]
    async fn reconciler_walks_past_a_batch_boundary() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        // More rows than one scan page (and than one delete chunk), with the
        // only referenced blob in the middle: both the keyset walk over the
        // blob rows and the chunked deletes must carry across the boundary.
        let total = Repository::BLOB_SCAN_BATCH as usize + Repository::BLOB_DELETE_CHUNK + 3;
        for i in 0..total {
            seed_old_blob(&repo, &format!("blob-{i:04}")).await;
        }
        seed_old_blob(&repo, "kept").await;
        put_object(&repo, "k", "kept").await;

        let removed = repo
            .reconcile_orphan_blobs(chrono::Local::now().to_utc())
            .await
            .expect("reconcile");

        assert_eq!(removed, total, "every unreferenced row must go");
        assert_eq!(blob_ids(&repo).await, ["kept"]);
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

        // A released reference reaching zero hands the id back to the caller;
        // the row itself is only dropped once the backend confirmed the
        // delete.
        let zero = repo
            .release_blob_refs(&["b1".to_string()])
            .await
            .expect("release");
        assert_eq!(zero, ["b1"]);
        assert_eq!(blob_ids(&repo).await, ["b1", "b2"]);

        repo.delete_released_blob_rows(&["b1".to_string()])
            .await
            .expect("delete rows");
        assert_eq!(blob_ids(&repo).await, ["b2"]);

        // A live reference is never taken away by a delete statement.
        repo.delete_released_blob_rows(&["b2".to_string()])
            .await
            .expect("no-op delete");
        assert_eq!(blob_ids(&repo).await, ["b2"]);
    }

    #[tokio::test]
    async fn release_counts_duplicate_ids_and_clamps_at_zero() {
        let repo = repo().await;
        repo.register_new_blob("b1".into(), 1)
            .await
            .expect("register");

        // One call can name the same blob many times. It must drop all of
        // its references and stop at zero.
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
        set_blob_refs(&repo, "b1", 2).await;

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

        // A concurrent copy can take the row back after the count reached
        // zero and before the caller claims it.
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
        set_blob_refs(&repo, "legacy", -3).await;

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

    /// A failure on one item must roll back the references that earlier
    /// items acquired.
    ///
    /// The caller cannot release them: it sees one error and no list of what
    /// was acquired, so a partial loop would leak every increment before the
    /// failing item.
    #[tokio::test]
    async fn acquire_is_all_or_nothing_when_an_item_fails() {
        let repo = repo().await;
        repo.db
            .execute_unprepared(
                "CREATE TRIGGER fail_bad_blob BEFORE INSERT ON s3_blob \
                 WHEN NEW.id = 'bad' \
                 BEGIN SELECT RAISE(ABORT, 'injected blob insert failure'); END",
            )
            .await
            .expect("create insert failure trigger");

        let result = repo
            .acquire_blob_refs(&[("good".to_string(), 1), ("bad".to_string(), 1)])
            .await;
        assert!(result.is_err(), "the failing item must fail the call");
        assert!(
            blob_ids(&repo).await.is_empty(),
            "no item may be acquired when a later item fails"
        );
    }
}
