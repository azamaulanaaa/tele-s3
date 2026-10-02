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

use crate::backend::{Backend, BackendError};

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
        // Matched by reference so the `Display` render of a variant can be
        // reused verbatim as the client-facing message instead of being
        // re-formatted here: one condition must have exactly one message.
        match &err {
            // Rate limiting is transient; surfacing SlowDown (503) lets AWS
            // SDK clients retry automatically instead of treating it as a
            // hard failure.
            BackendError::SlowDown => S3Error::new(s3s::S3ErrorCode::SlowDown),
            BackendError::ExceedLimitSize { .. } => {
                S3Error::with_message(s3s::S3ErrorCode::EntityTooLarge, err.to_string())
            }
            BackendError::OutOfRange => S3Error::new(s3s::S3ErrorCode::InvalidRange),
            // Permanent backend faults: no S3 code means "retrying cannot
            // help", so these stay 500 — but the message now names the
            // actual fault instead of the useless catch-all render.
            BackendError::PoisonedLock { .. } | BackendError::InvariantViolation { .. } => {
                S3Error::with_message(s3s::S3ErrorCode::InternalError, err.to_string())
            }
            // `BackendError` is `#[non_exhaustive]`: this arm must stay a
            // catch-all so a future variant is not a breaking change here.
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
    use crate::backend::{BoxedAsyncReader, Memory};

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

    #[async_trait::async_trait]
    impl Backend for FlakyDelete {
        async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError> {
            self.inner.write(size, reader).await
        }

        async fn read(
            &self,
            key: String,
            offset: u64,
            limit: Option<u64>,
        ) -> Result<Option<BoxedAsyncReader>, BackendError> {
            self.inner.read(key, offset, limit).await
        }

        async fn delete(&self, key: String) -> Result<(), BackendError> {
            if self.fail_delete.load(Ordering::SeqCst) {
                return Err(BackendError::Other("delete refused".into()));
            }
            self.inner.delete(key).await
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
        // so the client sees exactly what the backend reported.
        assert_eq!(err.message(), Some(message.as_str()));
    }

    #[test]
    fn permanent_backend_faults_are_named_internal_errors() {
        let poisoned: S3Error = BackendError::PoisonedLock {
            lock: "flood_guard",
        }
        .into();
        assert_eq!(*poisoned.code(), s3s::S3ErrorCode::InternalError);
        assert_eq!(
            poisoned.message(),
            Some("Internal lock `flood_guard` is poisoned")
        );

        let invariant: S3Error = BackendError::InvariantViolation {
            detail: "a free chunk slot was no longer free",
        }
        .into();
        assert_eq!(*invariant.code(), s3s::S3ErrorCode::InternalError);
        assert_eq!(
            invariant.message(),
            Some("Backend invariant violated: a free chunk slot was no longer free")
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
