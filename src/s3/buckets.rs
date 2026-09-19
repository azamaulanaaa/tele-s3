use s3s::{
    S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        Bucket, BucketLocationConstraint, BucketVersioningStatus, CreateBucketInput,
        CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput, GetBucketAclInput,
        GetBucketAclOutput, GetBucketLocationInput, GetBucketLocationOutput,
        GetBucketVersioningInput, GetBucketVersioningOutput, HeadBucketInput, HeadBucketOutput,
        ListBucketsInput, ListBucketsOutput, LocationType, PutBucketAclInput, PutBucketAclOutput,
        PutBucketVersioningInput, PutBucketVersioningOutput,
    },
};
use tracing::instrument;

use super::TeleS3;
use super::helpers::{canned_owner, chrono_to_timestamp, full_control_grant};
use crate::backend::Backend;

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(self), err)]
    pub(crate) async fn create_bucket_inner(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.repo
            .create_bucket(req.input.bucket, req.region.clone().map(|v| v.to_string()))
            .await?;

        let res = S3Response::new(CreateBucketOutput {
            location: req.region.map(|v| v.to_string()),
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_bucket_location_inner(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        let model = self.repo.get_bucket(&req.input.bucket).await?;

        let res = S3Response::new(GetBucketLocationOutput {
            location_constraint: model.region.map(BucketLocationConstraint::from),
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn list_buckets_inner(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.repo.list_buckets().await?;

        let buckets: Vec<Bucket> = buckets
            .into_iter()
            .map(|model| Bucket {
                name: Some(model.id),
                creation_date: Some(chrono_to_timestamp(model.created_at)),
                bucket_region: model.region,
            })
            .collect();

        let res = S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn delete_bucket_inner(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let is_exists = self.repo.bucket_exists(&req.input.bucket).await?;
        if !is_exists {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }

        let object_count = self.repo.get_bucket_object_count(&req.input.bucket).await?;
        if object_count > 0 {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }

        self.repo.delete_bucket(&req.input.bucket).await?;

        let res = S3Response::new(DeleteBucketOutput::default());

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn head_bucket_inner(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let model = self.repo.get_bucket(&req.input.bucket).await?;

        let res = S3Response::new(HeadBucketOutput {
            bucket_location_name: model.region.clone(),
            bucket_location_type: Some(LocationType::from_static(LocationType::LOCAL_ZONE)),
            bucket_region: model.region,
            ..Default::default()
        });
        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_bucket_acl_inner(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;

        Ok(S3Response::new(GetBucketAclOutput {
            owner: Some(canned_owner()),
            grants: Some(vec![full_control_grant()]),
        }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn put_bucket_acl_inner(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        // Existence-checked, otherwise accepted and ignored: this backend
        // has no ACL enforcement, but clients probing ACLs must not error.
        self.repo.get_bucket(&req.input.bucket).await?;

        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_bucket_versioning_inner(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let status = self.repo.get_bucket_versioning(&req.input.bucket).await?;
        let bucket_status = status.map(BucketVersioningStatus::from);
        Ok(S3Response::new(GetBucketVersioningOutput {
            status: bucket_status,
            ..Default::default()
        }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn put_bucket_versioning_inner(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let status = req.input.versioning_configuration.status.clone();
        let status_str = status.map(|s| s.as_str().to_string());
        // Validate: only Enabled or Suspended allowed
        if let Some(ref s) = status_str
            && s != "Enabled"
            && s != "Suspended"
        {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }
        self.repo
            .put_bucket_versioning(&req.input.bucket, status_str)
            .await?;
        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }
}
