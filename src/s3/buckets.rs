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
use super::helpers::{canned_owner, chrono_to_timestamp, full_control_grant, validate_bucket_name};
use crate::backend::Backend;

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(self), err)]
    pub(crate) async fn create_bucket_inner(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        validate_bucket_name(&req.input.bucket)?;

        match self
            .repo
            .create_bucket(req.input.bucket, req.region.clone().map(|v| v.to_string()))
            .await
        {
            Ok(()) => {}
            // Single-tenant gateway: every existing bucket is owned by the
            // requester, so recreating one is idempotent success (S3 200),
            // including races between concurrent creates.
            Err(err) if *err.code() == S3ErrorCode::BucketAlreadyExists => {}
            Err(err) => return Err(err),
        }

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

        // In-flight multipart uploads also block deletion, like S3.
        let upload_count = self.repo.get_bucket_upload_count(&req.input.bucket).await?;
        if upload_count > 0 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Memory;
    use crate::s3::TeleS3;

    fn create_req(bucket: &str) -> S3Request<CreateBucketInput> {
        S3Request {
            input: CreateBucketInput {
                bucket: bucket.to_string(),
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: format!("/{bucket}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn test_service() -> TeleS3<Memory<1, 1>> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(Memory::default(), db).await.expect("init")
    }

    #[tokio::test]
    async fn recreate_own_bucket_is_idempotent() {
        let svc = test_service().await;

        svc.create_bucket_inner(create_req("my-bucket"))
            .await
            .expect("first create");
        svc.create_bucket_inner(create_req("my-bucket"))
            .await
            .expect("second create should succeed idempotently");

        let buckets = svc.repo.list_buckets().await.expect("list");
        assert_eq!(
            buckets.iter().filter(|b| b.id == "my-bucket").count(),
            1,
            "duplicate bucket row created"
        );
    }

    #[tokio::test]
    async fn invalid_bucket_name_rejected() {
        let svc = test_service().await;

        let err = svc
            .create_bucket_inner(create_req("Invalid_Name"))
            .await
            .expect_err("should fail");
        assert_eq!(*err.code(), S3ErrorCode::InvalidBucketName);
    }

    #[tokio::test]
    async fn delete_bucket_blocked_by_inflight_upload() {
        let svc = test_service().await;

        svc.create_bucket_inner(create_req("mpu-bucket"))
            .await
            .expect("create");
        svc.repo
            .upsert_multipart_upload_state(
                "mpu-bucket".into(),
                "k".into(),
                "upload-1".into(),
                None,
                serde_json::json!({}),
                serde_json::json!([]),
                serde_json::json!({}),
            )
            .await
            .expect("create upload");

        let err = svc
            .delete_bucket_inner(S3Request {
                input: DeleteBucketInput {
                    bucket: "mpu-bucket".to_string(),
                    ..Default::default()
                },
                method: http::Method::DELETE,
                uri: "/mpu-bucket".parse().expect("uri"),
                headers: http::HeaderMap::new(),
                extensions: http::Extensions::default(),
                credentials: None,
                region: None,
                service: None,
                trailing_headers: None,
            })
            .await
            .expect_err("should fail");
        assert_eq!(*err.code(), S3ErrorCode::BucketNotEmpty);
    }
}
