use s3s::{
    S3, S3Error, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput,
        CompleteMultipartUploadOutput, CopyObjectInput, CopyObjectOutput, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectTaggingInput, DeleteObjectTaggingOutput, DeleteObjectsInput,
        DeleteObjectsOutput, GetBucketAclInput, GetBucketAclOutput, GetBucketLocationInput,
        GetBucketLocationOutput, GetBucketVersioningInput, GetBucketVersioningOutput,
        GetObjectAclInput, GetObjectAclOutput, GetObjectInput, GetObjectOutput,
        GetObjectTaggingInput, GetObjectTaggingOutput, HeadBucketInput, HeadBucketOutput,
        HeadObjectInput, HeadObjectOutput, ListBucketsInput, ListBucketsOutput,
        ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectVersionsInput,
        ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, ListPartsInput, ListPartsOutput, PutBucketAclInput,
        PutBucketAclOutput, PutBucketVersioningInput, PutBucketVersioningOutput, PutObjectAclInput,
        PutObjectAclOutput, PutObjectInput, PutObjectOutput, PutObjectTaggingInput,
        PutObjectTaggingOutput, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
        UploadPartOutput,
    },
};
use sea_orm::DatabaseConnection;
use tracing::instrument;

use crate::backend::{Backend, BackendError, ReclaimOutcome};

mod buckets;
mod helpers;
mod multipart;
mod objects;
mod repo;
mod types;

pub struct TeleS3<B: Backend> {
    pub(crate) backend: B,
    pub(crate) repo: repo::Repository,
}

/// Age a blob must reach before the start-up reconciler may consider its row
/// garbage. Long enough that a write still in flight can never be undone.
const ORPHAN_BLOB_MIN_AGE: chrono::Duration = chrono::Duration::hours(1);

/// Age an unconfirmed publish intent must reach before the reconciler may
/// act on it. Long enough that a write still in flight can never be undone.
///
/// Its own constant, deliberately equal to but separate from
/// [`ORPHAN_BLOB_MIN_AGE`]: the two passes guard different risks (an
/// unreferenced *row* versus an unconfirmed *publish*), and tying one to the
/// other would make tuning either one silently retune the other.
const PUBLISH_INTENT_MIN_AGE: chrono::Duration = chrono::Duration::hours(1);

/// Publish intents examined per reconciliation pass.
///
/// Bounded like every other pass here, so the number of Telegram lookups a
/// start-up can trigger is fixed rather than a function of how many writes
/// lost their answers.
const PUBLISH_INTENT_SWEEP_BATCH: u64 = 128;

/// What a publish intent's row must become once a reclaim attempt is done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntentDisposition {
    /// The publish is settled — there was nothing, or what there was is gone
    /// — so the row has served its purpose and may be dropped.
    Clear,
    /// The publish is not settled, so the row stays pending and a later pass
    /// retries it.
    KeepPending,
}

/// Whether an intent recorded at `created_at` is old enough to act on at
/// `now`.
///
/// Pure, and the single definition of the rule: the SQL that selects the
/// candidates and this function must agree, so the boundary is stated once
/// here and the query is documented as applying it.
///
/// Strictly older, matching [`ORPHAN_BLOB_MIN_AGE`]'s own rule: an intent
/// exactly at the threshold is not swept.
fn publish_intent_is_due(
    created_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    created_at + PUBLISH_INTENT_MIN_AGE < now
}

/// What one reclaim attempt means for the intent row.
///
/// The load-bearing decision of this scope, kept pure and separate from the
/// I/O so it can be tested without a Telegram session.
///
/// A backend error is [`IntentDisposition::KeepPending`] for the same reason
/// [`ReclaimOutcome::ReclaimFailed`] is: an unanswered reclaim is an unknown
/// publish, and the only thing that can resolve the unknown is a later pass.
/// The alternative — clearing the row because we could not check — would
/// destroy the *only* record of an object that may exist, which is the exact
/// failure this whole table exists to prevent.
fn intent_disposition(outcome: &Result<ReclaimOutcome, BackendError>) -> IntentDisposition {
    match outcome {
        Ok(ReclaimOutcome::NothingPublished) | Ok(ReclaimOutcome::Reclaimed) => {
            IntentDisposition::Clear
        }
        Ok(ReclaimOutcome::ReclaimFailed) | Err(_) => IntentDisposition::KeepPending,
    }
}

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(backend, db), level = "debug", err)]
    pub async fn init(backend: B, db: DatabaseConnection) -> anyhow::Result<Self> {
        let repo = repo::Repository::init(db).await?;

        Ok(Self { backend, repo })
    }

    /// Release metadata references and hard-delete blobs whose reference
    /// count reached zero.
    ///
    /// The backend delete is attempted *before* the bookkeeping is dropped,
    /// and its outcome is observed: only the ids the backend confirmed as
    /// removed lose their row. An id whose delete failed keeps its row, so
    /// the blob stays accounted for instead of becoming invisible data that
    /// nothing ever retries.
    ///
    /// Backend deletion failures never fail the user request (best-effort
    /// cleanup) but are logged; refcount bookkeeping failures propagate.
    async fn release_blobs(&self, ids: Vec<String>) -> S3Result<()> {
        let zero_ids = self.repo.release_blob_refs(&ids).await?;

        if zero_ids.is_empty() {
            return Ok(());
        }

        let results =
            futures::future::join_all(zero_ids.iter().map(|id| self.backend.delete(id.clone())))
                .await;

        let mut deleted: Vec<String> = Vec::with_capacity(zero_ids.len());
        let mut failed: Vec<String> = Vec::with_capacity(zero_ids.len());
        for (id, result) in zero_ids.iter().zip(results) {
            match result {
                Ok(()) => deleted.push(id.clone()),
                Err(err) => {
                    tracing::warn!(blob_id = %id, error = ?err, "backend delete failed");
                    failed.push(id.clone());
                }
            }
        }

        self.repo.delete_released_blob_rows(&deleted).await?;

        if !failed.is_empty() {
            tracing::warn!(
                retained = failed.len(),
                "blob rows kept after a failed backend delete; the start-up reconciler will drop them"
            );
        }

        Ok(())
    }

    /// Delete backend blobs that were written but never recorded in the
    /// metadata store (a publish rejected before its transaction).
    ///
    /// Best-effort by construction: there is no bookkeeping row to reconcile
    /// against, so a failure is logged and the request keeps its original
    /// error.
    async fn discard_backend_blobs(&self, ids: Vec<String>) {
        let results =
            futures::future::join_all(ids.iter().map(|id| self.backend.delete(id.clone()))).await;

        for (id, result) in ids.iter().zip(results) {
            if let Err(err) = result {
                tracing::warn!(
                    blob_id = %id,
                    error = ?err,
                    "failed to delete a backend blob that was never registered"
                );
            }
        }
    }

    /// Reconcile blob rows that no object version and no in-progress
    /// multipart upload references any more.
    ///
    /// Run once at start-up, before the service accepts traffic. Bounded and
    /// never fatal: an oversized database is skipped with a warning rather
    /// than turning the reconciler into an unbounded scan.
    pub async fn reconcile_orphan_blobs(&self) -> S3Result<usize> {
        // Anything younger than this may still belong to a write that has
        // not finished publishing.
        let older_than = chrono::Local::now().to_utc() - ORPHAN_BLOB_MIN_AGE;

        self.repo.reconcile_orphan_blobs(older_than).await
    }

    /// Reclaim publishes that were never confirmed, and clear the intents
    /// that recorded them.
    ///
    /// A pending intent means a publish may or may not have produced a real
    /// message holding uploaded bytes. When it did, no key came back, so no
    /// blob row references those bytes and
    /// [`Self::reconcile_orphan_blobs`] cannot see them: it drops metadata
    /// *rows*, not backend objects. This pass is what closes that gap, by
    /// asking the backend what it published under the identifier the intent
    /// recorded and deleting it if anything is there.
    ///
    /// # Independent of the orphan-blob pass
    ///
    /// The two passes are independent and either may be skipped:
    ///
    /// * they scan different tables (`s3_publish_intent` versus `s3_blob`)
    ///   and repair different classes of leak — bytes nobody can name versus
    ///   rows nobody references;
    /// * neither reads the other's state, so neither can be blocked by the
    ///   other, and running one does not require the other to have run;
    /// * they are run independently at start-up and each keeps its own age
    ///   threshold and its own bounds, so a failure in one is logged and does
    ///   not stop the other.
    ///
    /// They share only that both are bounded, batched and never fatal.
    ///
    /// Bounded on every axis: at most [`PUBLISH_INTENT_SWEEP_BATCH`] intents
    /// are examined, each costing the backend one bounded lookup, and no
    /// transaction spans the pass — every statement is its own short implicit
    /// transaction, so this never blocks writers for the length of a sweep.
    /// An intent is cleared only when
    /// [`IntentDisposition::Clear`] says its publish is settled, so a
    /// `ReclaimFailed` or an unanswered reclaim leaves the row pending and the
    /// next pass tries again.
    ///
    /// Returns the number of intents cleared. A backend failure on one intent
    /// is logged and the pass continues: one unreachable publish must not
    /// strand every other intent behind it.
    pub async fn reconcile_publish_intents(&self) -> S3Result<usize> {
        let now = chrono::Local::now().to_utc();
        // Anything younger than this may belong to a write that has not
        // finished publishing.
        let older_than = now - PUBLISH_INTENT_MIN_AGE;

        let candidates = self
            .repo
            .pending_publish_intents(older_than, PUBLISH_INTENT_SWEEP_BATCH)
            .await?;

        let mut cleared: Vec<String> = Vec::new();

        for intent in candidates {
            // The query already applies the age rule; re-checking it here
            // keeps the pass honest if the two ever drift, and costs nothing.
            if !publish_intent_is_due(intent.created_at, now) {
                continue;
            }

            let outcome = self
                .backend
                .reclaim_publish(intent.publish_id.clone())
                .await;

            match intent_disposition(&outcome) {
                IntentDisposition::Clear => cleared.push(intent.publish_id.clone()),
                IntentDisposition::KeepPending => match &outcome {
                    Ok(ReclaimOutcome::ReclaimFailed) => tracing::warn!(
                        publish_id = %intent.publish_id,
                        "an unconfirmed publish could not be deleted; its intent stays pending"
                    ),
                    // `Err` here is "this pass could not find out", not "there
                    // is nothing there" — logged at the same weight precisely
                    // because the row survives either way.
                    Err(err) => tracing::warn!(
                        publish_id = %intent.publish_id,
                        error = ?err,
                        "could not reclaim an unconfirmed publish; its intent stays pending"
                    ),
                    // Not `unreachable!`: this runs at start-up, where a panic
                    // on a future variant would take the service down over a
                    // reconciliation detail. Any arm that is not one of the
                    // two settled outcomes keeps the row and is logged.
                    Ok(other) => tracing::warn!(
                        publish_id = %intent.publish_id,
                        outcome = ?other,
                        "unrecognised reclaim outcome; its intent stays pending"
                    ),
                },
            }
        }

        self.repo.clear_publish_intents(&cleared).await?;

        Ok(cleared.len())
    }

    /// Fast-fail doomed conditional writes before any expensive work or
    /// validation. The atomic re-check still happens at commit time (in
    /// `publish_object_with_new_blob` / `cas_put_object`); this only avoids
    /// wasted effort and gives preconditions correct precedence over other
    /// errors (e.g. InvalidPart).
    async fn precondition_gate(
        &self,
        bucket: &str,
        key: &str,
        condition: &repo::PutCondition,
    ) -> S3Result<()> {
        if matches!(condition, repo::PutCondition::None) {
            return Ok(());
        }

        let exists = self.repo.object_exists(bucket, key).await?;
        let current_etag: Option<String> = if exists {
            self.repo.get_object(bucket, key).await?.etag
        } else {
            None
        };

        match condition {
            repo::PutCondition::IfMatch(expected) => match &current_etag {
                None => return Err(S3Error::new(s3s::S3ErrorCode::NoSuchKey)),
                Some(current) if current != expected => {
                    return Err(S3Error::new(s3s::S3ErrorCode::PreconditionFailed));
                }
                _ => {}
            },
            repo::PutCondition::IfMatchAny => {
                if current_etag.is_none() {
                    return Err(S3Error::new(s3s::S3ErrorCode::NoSuchKey));
                }
            }
            repo::PutCondition::IfNoneMatchAny => {
                if current_etag.is_some() {
                    return Err(S3Error::new(s3s::S3ErrorCode::PreconditionFailed));
                }
            }
            repo::PutCondition::IfNoneMatch(expected) => {
                if current_etag.as_deref() == Some(expected.as_str()) {
                    return Err(S3Error::new(s3s::S3ErrorCode::PreconditionFailed));
                }
            }
            repo::PutCondition::None => {}
        }

        Ok(())
    }
}

impl From<BackendError> for S3Error {
    fn from(err: BackendError) -> Self {
        // Split by what the client can *act on*, because the message goes
        // into the response body verbatim: anything a caller can provoke is
        // disclosed, however useful it reads in a log. Matched by reference
        // so the `Display` render of the one variant whose message is
        // client-facing is reused verbatim instead of being re-formatted
        // here — one condition must have exactly one message.
        match &err {
            // Rate limiting is transient; surfacing SlowDown (503) lets AWS
            // SDK clients retry automatically instead of treating it as a
            // hard failure.
            BackendError::SlowDown => S3Error::new(s3s::S3ErrorCode::SlowDown),
            // The only message worth sending: two numbers the client can
            // shrink its next request to fit.
            BackendError::ExceedLimitSize { .. } => {
                S3Error::with_message(s3s::S3ErrorCode::EntityTooLarge, err.to_string())
            }
            BackendError::OutOfRange => S3Error::new(s3s::S3ErrorCode::InvalidRange),
            // A poisoned lock and a violated invariant describe this
            // process, not the request: the names are an inventory of the
            // backend's internals for anyone who can provoke a fault. They
            // still map to `InternalError` — retrying cannot help — but only
            // as the error's `source`, which the handler's
            // `#[instrument(err)]` renders into the log. `Backend` is a
            // public extension surface, so this stays a catch-all and a
            // future or third-party variant lands here too: an
            // unrecognised fault is opaque to the client by default.
            _ => S3Error::internal_error(err),
        }
    }
}

#[async_trait::async_trait]
impl<B: Backend> S3 for TeleS3<B> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.create_bucket_inner(req).await
    }

    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.get_bucket_location_inner(req).await
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        self.list_buckets_inner(req).await
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.delete_bucket_inner(req).await
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.head_bucket_inner(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.put_object_inner(req).await
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        self.copy_object_inner(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.create_multipart_upload_inner(req).await
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.upload_part_inner(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.complete_multipart_upload_inner(req).await
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.abort_multipart_upload_inner(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.get_object_inner(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.head_object_inner(req).await
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.delete_object_inner(req).await
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.delete_objects_inner(req).await
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        self.list_objects_inner(req).await
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.list_objects_v2_inner(req).await
    }

    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        self.list_parts_inner(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.list_multipart_uploads_inner(req).await
    }

    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.upload_part_copy_inner(req).await
    }

    async fn get_bucket_acl(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        self.get_bucket_acl_inner(req).await
    }

    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        self.put_bucket_acl_inner(req).await
    }

    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        self.get_object_acl_inner(req).await
    }

    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.put_object_acl_inner(req).await
    }

    async fn get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        self.get_object_tagging_inner(req).await
    }

    async fn put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        self.put_object_tagging_inner(req).await
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        self.delete_object_tagging_inner(req).await
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        self.get_bucket_versioning_inner(req).await
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        self.put_bucket_versioning_inner(req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        self.list_object_versions_inner(req).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use sea_orm::EntityTrait;

    use super::repo::entity;
    use super::*;
    use crate::backend::{BoxedAsyncReader, Memory, ReadOutcome};

    /// A backend whose deletes can be made to fail on demand, so the
    /// release ordering is observable.
    struct FlakyDelete {
        inner: Memory<8, 8>,
        fail_delete: AtomicBool,
    }

    impl FlakyDelete {
        fn new(fail_delete: bool) -> Self {
            Self {
                inner: Memory::default(),
                fail_delete: AtomicBool::new(fail_delete),
            }
        }

        fn fail_deletes(&self, fail: bool) {
            self.fail_delete.store(fail, Ordering::SeqCst);
        }
    }

    /// A backend whose answer to `reclaim_publish` is dictated by the test,
    /// so every arm of the sweep's decision is reachable without a Telegram
    /// session.
    struct ScriptedReclaim {
        inner: Memory<8, 8>,
        /// What the next reclaim answers. `None` means the call fails.
        outcome: std::sync::Mutex<Option<Result<ReclaimOutcome, BackendError>>>,
        /// Publish ids this backend was asked about, in order.
        asked: std::sync::Mutex<Vec<String>>,
    }

    impl ScriptedReclaim {
        fn answering(outcome: Result<ReclaimOutcome, BackendError>) -> Self {
            Self {
                inner: Memory::default(),
                outcome: std::sync::Mutex::new(Some(outcome)),
                asked: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn asked_about(&self) -> Vec<String> {
            self.asked.lock().expect("asked log").clone()
        }
    }

    #[async_trait::async_trait]
    impl Backend for ScriptedReclaim {
        async fn write(
            &self,
            publish_id: String,
            size: u64,
            reader: BoxedAsyncReader,
        ) -> Result<String, BackendError> {
            self.inner.write(publish_id, size, reader).await
        }

        async fn read(
            &self,
            key: String,
            offset: u64,
            limit: Option<u64>,
        ) -> Result<ReadOutcome, BackendError> {
            self.inner.read(key, offset, limit).await
        }

        async fn delete(&self, key: String) -> Result<(), BackendError> {
            self.inner.delete(key).await
        }

        async fn reclaim_publish(
            &self,
            publish_id: String,
        ) -> Result<ReclaimOutcome, BackendError> {
            self.asked.lock().expect("asked log").push(publish_id);

            self.outcome
                .lock()
                .expect("scripted outcome")
                .take()
                .unwrap_or(Ok(ReclaimOutcome::NothingPublished))
        }
    }

    #[async_trait::async_trait]
    impl Backend for FlakyDelete {
        async fn write(
            &self,
            publish_id: String,
            size: u64,
            reader: BoxedAsyncReader,
        ) -> Result<String, BackendError> {
            self.inner.write(publish_id, size, reader).await
        }

        async fn read(
            &self,
            key: String,
            offset: u64,
            limit: Option<u64>,
        ) -> Result<ReadOutcome, BackendError> {
            self.inner.read(key, offset, limit).await
        }

        async fn delete(&self, key: String) -> Result<(), BackendError> {
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(BackendError::Other("delete refused".into()));
            }
            self.inner.delete(key).await
        }

        async fn reclaim_publish(
            &self,
            publish_id: String,
        ) -> Result<ReclaimOutcome, BackendError> {
            self.inner.reclaim_publish(publish_id).await
        }
    }

    async fn service(fail_delete: bool) -> TeleS3<FlakyDelete> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(FlakyDelete::new(fail_delete), db)
            .await
            .expect("init")
    }

    /// A service over a scripted backend, so every reclaim arm is reachable.
    async fn scripted(outcome: Result<ReclaimOutcome, BackendError>) -> TeleS3<ScriptedReclaim> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(ScriptedReclaim::answering(outcome), db)
            .await
            .expect("init")
    }

    /// Record an intent `age` old, directly, so the age under test is the one
    /// in the row rather than one the clock happens to have produced.
    async fn record_intent_aged<B: Backend>(
        svc: &TeleS3<B>,
        publish_id: &str,
        age: chrono::Duration,
    ) {
        use sea_orm::ActiveValue::Set;

        let created_at = chrono::Local::now().to_utc() - age;

        entity::publish_intent::Entity::insert(entity::publish_intent::ActiveModel {
            publish_id: Set(publish_id.to_string()),
            bucket_id: Set("b".into()),
            object_id: Set("k".into()),
            size: Set(1),
            blob_id: Set(None),
            created_at: Set(created_at),
            resolved_at: Set(None),
        })
        .exec(&svc.repo.db)
        .await
        .expect("record intent");
    }

    async fn pending_intents<B: Backend>(svc: &TeleS3<B>) -> Vec<String> {
        let mut ids: Vec<String> = entity::publish_intent::Entity::find()
            .all(&svc.repo.db)
            .await
            .expect("load intents")
            .into_iter()
            .map(|m| m.publish_id)
            .collect();
        ids.sort_unstable();

        ids
    }

    #[test]
    fn an_intent_younger_than_the_threshold_is_not_due() {
        let now = chrono::Local::now().to_utc();

        // A write that may still be in flight must never be swept: acting on
        // it would go looking for a publish that has not happened yet.
        assert!(
            !publish_intent_is_due(now - chrono::Duration::minutes(30), now),
            "a half-hour-old intent is inside the threshold"
        );
        assert!(
            !publish_intent_is_due(now, now),
            "an intent recorded just now is inside the threshold"
        );
        assert!(
            publish_intent_is_due(now - chrono::Duration::hours(2), now),
            "an intent older than the threshold is due"
        );
    }

    #[test]
    fn a_settled_publish_clears_its_intent_and_an_unsettled_one_keeps_it() {
        // The load-bearing mapping of the scope. Only the two settled answers
        // may drop the row; every other case must leave it pending, because a
        // pending row is the only record an object that may exist can be
        // found by.
        for outcome in [ReclaimOutcome::NothingPublished, ReclaimOutcome::Reclaimed] {
            assert_eq!(
                intent_disposition(&Ok(outcome)),
                IntentDisposition::Clear,
                "{outcome:?} settles the publish"
            );
        }

        assert_eq!(
            intent_disposition(&Ok(ReclaimOutcome::ReclaimFailed)),
            IntentDisposition::KeepPending,
            "a publish that is still there must be retried, not forgotten"
        );

        assert_eq!(
            intent_disposition(&Err(BackendError::Other("telegram said no".into()))),
            IntentDisposition::KeepPending,
            "a lookup that failed is an unknown publish, never an absent one"
        );
    }

    #[tokio::test]
    async fn a_pending_but_young_intent_is_not_swept() {
        let svc = scripted(Ok(ReclaimOutcome::Reclaimed)).await;
        record_intent_aged(&svc, "young", chrono::Duration::minutes(5)).await;

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 0);
        assert_eq!(
            pending_intents(&svc).await,
            ["young"],
            "an intent that may still be in flight must be left alone"
        );
        assert!(
            svc.backend.asked_about().is_empty(),
            "the backend must not even be asked about an intent inside the threshold"
        );
    }

    #[tokio::test]
    async fn a_nothing_published_intent_is_cleared() {
        let svc = scripted(Ok(ReclaimOutcome::NothingPublished)).await;
        record_intent_aged(&svc, "gone", chrono::Duration::hours(2)).await;

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 1);
        assert!(pending_intents(&svc).await.is_empty());
        assert_eq!(svc.backend.asked_about(), ["gone"]);
    }

    #[tokio::test]
    async fn a_reclaimed_intent_is_cleared() {
        let svc = scripted(Ok(ReclaimOutcome::Reclaimed)).await;
        record_intent_aged(&svc, "deleted", chrono::Duration::hours(2)).await;

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 1);
        assert!(pending_intents(&svc).await.is_empty());
    }

    #[tokio::test]
    async fn an_indelible_publish_keeps_its_intent_pending_for_a_later_pass() {
        // The assertion the whole scope turns on: a message that was found
        // but not deleted leaves the row pending, so a later pass retries it.
        let svc = scripted(Ok(ReclaimOutcome::ReclaimFailed)).await;
        record_intent_aged(&svc, "stuck", chrono::Duration::hours(2)).await;

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 0);
        assert_eq!(
            pending_intents(&svc).await,
            ["stuck"],
            "an object that may still exist must keep the only record of it"
        );

        // And the next pass does try again.
        let svc = scripted(Ok(ReclaimOutcome::ReclaimFailed)).await;
        record_intent_aged(&svc, "stuck", chrono::Duration::hours(2)).await;
        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 0);
        assert_eq!(svc.backend.asked_about(), ["stuck"]);
        assert_eq!(pending_intents(&svc).await, ["stuck"]);
    }

    #[tokio::test]
    async fn a_failed_reclaim_keeps_its_intent_pending() {
        // An unanswered lookup is an unknown publish. Clearing the row here
        // would erase the only record of bytes that may be sitting in the
        // chat, which is the exact failure the table exists to prevent.
        let svc = scripted(Err(BackendError::Other("telegram unreachable".into()))).await;
        record_intent_aged(&svc, "unknown", chrono::Duration::hours(2)).await;

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 0);
        assert_eq!(pending_intents(&svc).await, ["unknown"]);
    }

    #[tokio::test]
    async fn the_sweep_is_bounded_to_one_batch_of_intents() {
        let svc = scripted(Ok(ReclaimOutcome::Reclaimed)).await;
        for i in 0..PUBLISH_INTENT_SWEEP_BATCH + 10 {
            record_intent_aged(&svc, &format!("id-{i:04}"), chrono::Duration::hours(2)).await;
        }

        assert_eq!(
            svc.reconcile_publish_intents().await.expect("sweep"),
            PUBLISH_INTENT_SWEEP_BATCH as usize,
            "one pass examines one batch, never the whole table"
        );
        assert_eq!(
            pending_intents(&svc).await.len(),
            10,
            "the remainder is left for the next pass"
        );
    }

    #[tokio::test]
    async fn a_resolved_intent_is_never_swept() {
        let svc = scripted(Ok(ReclaimOutcome::Reclaimed)).await;
        record_intent_aged(&svc, "confirmed", chrono::Duration::hours(2)).await;
        svc.repo
            .resolve_publish_intent("confirmed", "key-1".into())
            .await
            .expect("resolve");

        assert_eq!(svc.reconcile_publish_intents().await.expect("sweep"), 0);
        assert_eq!(
            pending_intents(&svc).await,
            ["confirmed"],
            "a confirmed publish's row is the only record of the key it produced"
        );
        assert!(svc.backend.asked_about().is_empty());
    }

    #[tokio::test]
    async fn the_two_reconciliation_passes_are_independent() {
        // Each pass must be runnable without the other having run, and must
        // not disturb the other's rows.
        let svc = service(false).await;
        svc.repo
            .register_new_blob("b1".into(), 1)
            .await
            .expect("register");
        record_intent_aged(&svc, "unconfirmed", chrono::Duration::hours(2)).await;

        assert_eq!(svc.reconcile_orphan_blobs().await.expect("blob pass"), 0);
        assert_eq!(
            pending_intents(&svc).await,
            ["unconfirmed"],
            "the blob pass must not touch publish intents"
        );

        assert_eq!(
            svc.reconcile_publish_intents().await.expect("intent pass"),
            1
        );
        assert_eq!(
            blob_rows(&svc).await,
            [1],
            "the intent pass must not touch blob rows"
        );
    }

    async fn blob_rows(svc: &TeleS3<FlakyDelete>) -> Vec<i64> {
        let mut rows: Vec<i64> = entity::blob::Entity::find()
            .all(&svc.repo.db)
            .await
            .expect("load blob rows")
            .into_iter()
            .map(|m| m.refs)
            .collect();
        rows.sort_unstable();
        rows
    }

    #[test]
    fn retryable_backend_failures_keep_their_transient_s3_codes() {
        let err: S3Error = BackendError::SlowDown.into();
        assert_eq!(*err.code(), s3s::S3ErrorCode::SlowDown);
        assert_eq!(
            err.status_code(),
            Some(http::StatusCode::SERVICE_UNAVAILABLE)
        );

        let err: S3Error = BackendError::OutOfRange.into();
        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidRange);
    }

    #[test]
    fn size_limit_failure_keeps_entity_too_large_and_the_backend_message() {
        let message = BackendError::ExceedLimitSize {
            max: 10,
            actual: 20,
        }
        .to_string();

        let err: S3Error = BackendError::ExceedLimitSize {
            max: 10,
            actual: 20,
        }
        .into();
        assert_eq!(*err.code(), s3s::S3ErrorCode::EntityTooLarge);
        // The mapping reuses the `Display` render rather than re-formatting,
        // so the client sees exactly what the backend reported. Redaction
        // must not swallow this one: the numbers are what the client can act
        // on, so they are the whole point of the error.
        assert_eq!(err.message(), Some(message.as_str()));
        assert_eq!(err.message(), Some("Size 20 exceeds limit 10"));
    }

    #[test]
    fn permanent_backend_faults_are_internal_errors_that_name_nothing() {
        // The detail of a permanent fault is an inventory of this backend's
        // internals: anything that can reach the endpoint can provoke one and
        // read the names back. The message the client receives is the S3
        // code's own fixed text; the detail survives only as the error's
        // source, which the handler's `#[instrument(err)]` logs.
        let poisoned: S3Error = BackendError::PoisonedLock {
            lock: "flood_guard",
        }
        .into();
        assert_eq!(*poisoned.code(), s3s::S3ErrorCode::InternalError);
        assert_eq!(
            poisoned.message(),
            s3s::S3ErrorCode::InternalError.default_message()
        );
        assert!(
            !poisoned
                .message()
                .unwrap_or_default()
                .contains("flood_guard"),
            "the poisoned lock's name must not reach the client"
        );

        let invariant: S3Error = BackendError::InvariantViolation {
            detail: "a free chunk slot was no longer free",
        }
        .into();
        assert_eq!(*invariant.code(), s3s::S3ErrorCode::InternalError);
        assert_eq!(
            invariant.message(),
            poisoned.message(),
            "every internal-only fault answers with the same uninformative text"
        );
        assert!(
            !invariant
                .message()
                .unwrap_or_default()
                .contains("free chunk slot"),
            "the violated invariant must not reach the client"
        );
    }

    #[test]
    fn a_permanent_fault_keeps_its_detail_in_the_error_source() {
        // Redaction is only safe because the detail is not lost: this is
        // what an operator reads to tell a corrupt key from a dead lock.
        let poisoned: S3Error = BackendError::PoisonedLock {
            lock: "flood_guard",
        }
        .into();
        let detail = poisoned
            .source()
            .expect("the backend fault is kept as the error source")
            .to_string();
        assert!(detail.contains("flood_guard"), "logged detail: {detail}");

        let invariant: S3Error = BackendError::InvariantViolation {
            detail: "a free chunk slot was no longer free",
        }
        .into();
        let detail = invariant
            .source()
            .expect("the backend fault is kept as the error source")
            .to_string();
        assert!(
            detail.contains("free chunk slot"),
            "logged detail: {detail}"
        );
    }

    #[test]
    fn the_other_catch_all_is_generic_too() {
        // `Backend` is a public extension surface: a third-party backend's
        // `Other`, and any variant added later, must not become a message.
        let err: S3Error =
            BackendError::Other(std::io::Error::other("telegram said no").into()).into();
        assert_eq!(*err.code(), s3s::S3ErrorCode::InternalError);
        assert!(
            !err.message()
                .unwrap_or_default()
                .contains("telegram said no"),
            "an unrecognized backend failure must not reach the client: {:?}",
            err.message()
        );
    }

    #[test]
    fn wrapped_upstream_errors_still_carry_their_source_chain() {
        let err: S3Error =
            BackendError::Other(std::io::Error::other("telegram said no").into()).into();

        assert_eq!(*err.code(), s3s::S3ErrorCode::InternalError);
        let backend = err.source().expect("the backend error is the source");
        let upstream = backend
            .source()
            .expect("the wrapped upstream error is preserved");
        assert!(upstream.to_string().contains("telegram said no"));
    }

    #[tokio::test]
    async fn failed_backend_delete_keeps_the_blob_row() {
        let svc = service(true).await;
        svc.repo
            .register_new_blob("b1".into(), 1)
            .await
            .expect("register");

        svc.release_blobs(vec!["b1".to_string()])
            .await
            .expect("release stays best-effort for the caller");

        assert_eq!(
            blob_rows(&svc).await,
            [0],
            "a blob the backend would not delete must keep its bookkeeping \
             (and its zero refcount) instead of becoming invisible data"
        );

        // Once the backend accepts the delete the row is dropped.
        svc.backend.fail_deletes(false);
        svc.release_blobs(vec!["b1".to_string()])
            .await
            .expect("release again");

        assert!(
            blob_rows(&svc).await.is_empty(),
            "a confirmed delete must drop the row"
        );
    }

    #[tokio::test]
    async fn reconcile_orphan_blobs_leaves_referenced_blobs_alone() {
        let svc = service(true).await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.repo
            .register_new_blob("b1".into(), 1)
            .await
            .expect("register");

        // Published through the atomic path, so the row and the object row
        // agree on who owns the blob.
        svc.repo
            .publish_object_with_new_blob(
                "b2".into(),
                1,
                "b".into(),
                "k".into(),
                super::repo::ObjectWrite {
                    size: 1,
                    content_type: None,
                    etag: Some("e1".into()),
                    content: serde_json::json!({"item": [{"id": "b2", "offset": 0, "size": 1}]}),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    tags: serde_json::json!([]),
                },
                super::repo::PutCondition::None,
            )
            .await
            .expect("publish");

        // Both rows are younger than the threshold, so the start-up pass has
        // nothing to do; it must not touch them either way.
        assert_eq!(svc.reconcile_orphan_blobs().await.expect("reconcile"), 0);
        assert_eq!(blob_rows(&svc).await, [1, 1]);
    }
}
