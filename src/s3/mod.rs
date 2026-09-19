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

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(backend, db), level = "debug", err)]
    pub async fn init(backend: B, db: DatabaseConnection) -> anyhow::Result<Self> {
        let repo = repo::Repository::init(db).await?;

        Ok(Self { backend, repo })
    }

    /// Release metadata references and hard-delete blobs whose reference
    /// count reached zero.
    ///
    /// Backend deletion failures are ignored (best-effort cleanup);
    /// refcount bookkeeping failures propagate.
    async fn release_blobs(&self, ids: Vec<String>) -> S3Result<()> {
        let zero_ids = self.repo.release_blob_refs(&ids).await?;
        let delete_futures = zero_ids.into_iter().map(|id| self.backend.delete(id));
        let _ = futures::future::join_all(delete_futures).await;

        Ok(())
    }

    /// Fast-fail doomed conditional writes before any expensive work or
    /// validation. The atomic re-check still happens in cas_put_object at
    /// commit time; this only avoids wasted effort and gives preconditions
    /// correct precedence over other errors (e.g. InvalidPart).
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
        match err {
            // Rate limiting is transient; surfacing SlowDown (503) lets AWS
            // SDK clients retry automatically instead of treating it as a
            // hard failure.
            BackendError::SlowDown => S3Error::new(s3s::S3ErrorCode::SlowDown),
            other => S3Error::internal_error(other),
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
