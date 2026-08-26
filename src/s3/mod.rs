use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use base64::Engine;
use digest::Digest;
use futures::{io::AsyncReadExt, TryStreamExt};
use s3s::{
    S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket, BucketLocationConstraint,
        CommonPrefix, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CopyObjectInput,
        CopyObjectOutput, CopyObjectResult, CopyPartResult, CopySource, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag, ETagCondition,
        GetBucketAclInput, GetBucketAclOutput, GetBucketLocationInput, GetBucketLocationOutput,
        GetObjectAclInput, GetObjectAclOutput, GetObjectInput, GetObjectOutput, Grant, Grantee,
        HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput,
        ListBucketsOutput, ListMultipartUploadsInput, ListMultipartUploadsOutput, ListObjectsInput,
        ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, ListPartsInput,
        ListPartsOutput, LocationType, MultipartUpload, Object, Owner, Part, PutBucketAclInput,
        PutBucketAclOutput, PutObjectAclInput, PutObjectAclOutput, PutObjectInput, PutObjectOutput,
        StreamingBlob, Timestamp, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput,
        UploadPartOutput,
    },
};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    backend::{Backend, BackendError, BoxedAsyncReader, ChainReaders, ReaderWithHasher},
};
use repo::{ObjectWrite, PutCondition, Repository};

mod repo;

pub struct TeleS3<B: Backend> {
    backend: B,
    repo: Repository,
}

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(backend, db), level = "debug", err)]
    pub async fn init(backend: B, db: DatabaseConnection) -> anyhow::Result<Self> {
        let repo = Repository::init(db).await?;

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
        condition: &PutCondition,
    ) -> S3Result<()> {
        if matches!(condition, PutCondition::None) {
            return Ok(());
        }

        let exists = self.repo.object_exists(bucket, key).await?;
        let current_etag: Option<String> = if exists {
            self.repo.get_object(bucket, key).await?.etag
        } else {
            None
        };

        match condition {
            PutCondition::IfMatch(expected) => match &current_etag {
                None => return Err(S3Error::new(S3ErrorCode::NoSuchKey)),
                Some(current) if current != expected => {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
                _ => {}
            },
            PutCondition::IfMatchAny => {
                if current_etag.is_none() {
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
            }
            PutCondition::IfNoneMatchAny => {
                if current_etag.is_some() {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
            PutCondition::IfNoneMatch(expected) => {
                if current_etag.as_deref() == Some(expected.as_str()) {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
            PutCondition::None => {}
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
            BackendError::SlowDown => S3Error::new(S3ErrorCode::SlowDown),
            other => S3Error::internal_error(other),
        }
    }
}

#[async_trait::async_trait]
impl<B: Backend> S3 for TeleS3<B> {
    #[instrument(skip(self), err)]
    async fn create_bucket(
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
    async fn get_bucket_location(
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
    async fn list_buckets(
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
    async fn delete_bucket(
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
    async fn head_bucket(
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
    async fn put_object(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let size =
            req.input
                .content_length
                .ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))? as u64;

        let condition = build_put_condition(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )?;

        // Fast-fail doomed conditional writes before streaming into the
        // backend; the atomic re-check still happens in cas_put_object.
        self.precondition_gate(&req.input.bucket, &req.input.key, &condition)
            .await?;

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        let (id, hash_md5) = {
            let hasher_md5 = Arc::new(Mutex::new(md5::Md5::new()));

            let id = if size > 0 {
                let reader_with_hasher =
                    Box::pin(ReaderWithHasher::new(reader, hasher_md5.clone()));

                let id = self
                    .backend
                    .write(size, reader_with_hasher)
                    .await
                    .map_err(S3Error::from)?;
                Some(id)
            } else {
                None
            };

            let hash_md5 = hasher_md5
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .finalize_reset();

            (id, hash_md5)
        };

        // The backend blob now exists and is owned solely by the incoming
        // object until it replaces a previous version.
        if let Some(ref id) = id {
            self.repo.register_new_blob(id.clone(), size).await?;
        }

        if let Some(expected) = req.input.content_md5 {
            // Decode instead of comparing strings so padding/whitespace
            // variations in the client header don't cause false mismatches.
            let expected_digest = match base64::prelude::BASE64_STANDARD.decode(expected.trim()) {
                Ok(v) => v,
                Err(_) => return Err(S3Error::new(S3ErrorCode::InvalidDigest)),
            };

            if expected_digest[..] != hash_md5[..] {
                if let Some(id) = id {
                    self.release_blobs(vec![id]).await?;
                }

                return Err(S3Error::new(S3ErrorCode::BadDigest));
            }
        }

        let etag = Some(hex::encode(hash_md5));

        let content_json = {
            let mut content = Metadata { item: vec![] };

            if let Some(id) = id.clone() {
                content.item.push(MetadataItem {
                    id,
                    offset: 0,
                    size,
                })
            }

            serde_json::to_value(&content).map_err(S3Error::internal_error)?
        };

        let delete_old_future = {
            let is_exists = self
                .repo
                .object_exists(&req.input.bucket, &req.input.key)
                .await?;

            if is_exists {
                let model = self
                    .repo
                    .get_object(&req.input.bucket, &req.input.key)
                    .await?;

                let metadata: Metadata =
                    serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

                // Refcount-aware: shared blobs survive if something else
                // still references them.
                Some(self.release_blobs(metadata.item.into_iter().map(|v| v.id).collect()))
            } else {
                None
            }
        };

        let data = ObjectWrite {
            size,
            content_type: req.input.content_type.take(),
            etag: etag.clone(),
            content: content_json,
            user_metadata: metadata_to_json(req.input.metadata.take()),
        };

        {
            let result = self
                .repo
                .cas_put_object(req.input.bucket, req.input.key, data, condition)
                .await;

            if let Err(err) = result {
                if let Some(id) = id {
                    // Best-effort cleanup only: a cleanup failure must not
                    // replace the original error the client should see.
                    let _ = self.release_blobs(vec![id]).await;
                }

                return Err(err);
            }
        }

        if let Some(delete_old_future) = delete_old_future {
            delete_old_future.await?;
        }

        let res = S3Response::new(PutObjectOutput {
            e_tag: etag.map(ETag::Strong),
            size: Some(size as i64),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // Access-point sources are not supported.
        let (src_bucket, src_key) = match &req.input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (&**bucket, &**key),
            CopySource::AccessPoint { .. } => {
                return Err(S3Error::new(S3ErrorCode::NotImplemented));
            }
        };

        let model = self.repo.get_object(src_bucket, src_key).await?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
        let size = model.size as u64;

        // Point the destination at the source's existing backend blobs
        // instead of duplicating them: backends may be capacity-limited,
        // Telegram uploads are expensive, and S3 semantics only require the
        // destination to expose equivalent content.
        //
        // Acquire references BEFORE releasing anything, so copying an
        // object onto itself (or an alias of it) nets out safely.
        let acquire_items: Vec<(String, u64)> = metadata
            .item
            .iter()
            .map(|v| (v.id.clone(), v.size))
            .collect();
        self.repo.acquire_blob_refs(&acquire_items).await?;

        let content_json = serde_json::to_value(&metadata).map_err(S3Error::internal_error)?;

        let delete_old_future = {
            if let Ok(old) = self
                .repo
                .get_object(&req.input.bucket, &req.input.key)
                .await
            {
                let old_metadata: Metadata =
                    serde_json::from_value(old.content).map_err(S3Error::internal_error)?;

                // Refcount-aware: blobs still referenced elsewhere survive.
                Some(
                    self.release_blobs(old_metadata.item.into_iter().map(|v| v.id).collect()),
                )
            } else {
                None
            }
        };

        let data = ObjectWrite {
            size,
            content_type: model.content_type.clone(),
            etag: model.etag.clone(),
            content: content_json,
            // AWS MetadataDirective defaults to COPY; carry source metadata.
            user_metadata: model.user_metadata.clone(),
        };

        self.repo
            .upsert_object(req.input.bucket.clone(), req.input.key.clone(), data)
            .await?;

        // On an upsert failure the acquired refs above are simply kept:
        // over-protecting blobs is safe, under-protecting them is not.
        if let Some(delete_old_future) = delete_old_future {
            delete_old_future.await?;
        }

        let res = S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: model.etag.map(ETag::Strong),
                last_modified: Some(chrono_to_timestamp(model.last_modified)),
                ..Default::default()
            }),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;

        let upload_id = uuid::Uuid::new_v4().to_string();

        let content = BTreeMap::<i32, MultipartUploadPart>::new();
        let content_json = serde_json::to_value(&content).map_err(S3Error::internal_error)?;

        self.repo
            .upsert_multipart_upload_state(
                req.input.bucket.clone(),
                req.input.key.clone(),
                upload_id.clone(),
                req.input.content_type,
                metadata_to_json(req.input.metadata.clone()),
                content_json,
            )
            .await?;

        let res = S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let _model = self
            .repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let size =
            req.input
                .content_length
                .ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))? as u64;

        if size == 0 {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        let (id, hash_md5) = {
            let hasher_md5 = Arc::new(Mutex::new(md5::Md5::new()));

            let reader_with_hasher = Box::pin(ReaderWithHasher::new(reader, hasher_md5.clone()));

            let id = self
                .backend
                .write(size, reader_with_hasher)
                .await
                .map_err(S3Error::from)?;

            let hash_md5 = hasher_md5
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .finalize_reset();

            (id, hash_md5)
        };

        // The part blob is owned by the multipart upload state until the
        // upload completes (ownership moves to the object) or aborts.
        self.repo.register_new_blob(id.clone(), size).await?;

        if let Some(expected) = req.input.content_md5 {
            let expected_digest = match base64::prelude::BASE64_STANDARD.decode(expected.trim()) {
                Ok(v) => v,
                Err(_) => return Err(S3Error::new(S3ErrorCode::InvalidDigest)),
            };

            if expected_digest[..] != hash_md5[..] {
                self.release_blobs(vec![id]).await?;

                return Err(S3Error::new(S3ErrorCode::BadDigest));
            }
        }

        let multipart_upload_part = MultipartUploadPart {
            hash: hex::encode(hash_md5),
            metadata_items: vec![MetadataItem {
                id,
                offset: 0,
                size,
            }],
        };

        self.repo
            .cas_update_multipart_content(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                |content| {
                    let mut parts: BTreeMap<i32, MultipartUploadPart> =
                        serde_json::from_value(content.take())
                            .map_err(S3Error::internal_error)?;

                    parts.insert(req.input.part_number, multipart_upload_part.clone());

                    *content = serde_json::to_value(parts).map_err(S3Error::internal_error)?;

                    Ok(())
                },
            )
            .await?;

        let res = S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(multipart_upload_part.hash)),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let condition = build_put_condition(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )?;

        // Preconditions take precedence over other completion errors, so
        // evaluate them before touching parts; the atomic re-check happens
        // in cas_put_object at commit time.
        self.precondition_gate(&req.input.bucket, &req.input.key, &condition)
            .await?;

        let requested_parts = req
            .input
            .multipart_upload
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?
            .parts
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?
            .into_iter()
            .map(|v| v.part_number)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

        let model = self
            .repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let mut content =
            serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(model.content)
                .map_err(S3Error::internal_error)?;

        let filtered_content = requested_parts
            .iter()
            .map(|index| content.remove(index))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

        let metadata_json = {
            let metadata_items = filtered_content
                .iter()
                .flat_map(|v| v.metadata_items.clone())
                .collect::<Vec<_>>();
            let metadata = Metadata {
                item: metadata_items,
            };

            serde_json::to_value(&metadata).map_err(S3Error::internal_error)?
        };

        let size: u64 = filtered_content
            .iter()
            .flat_map(|v| v.metadata_items.iter().map(|i| i.size))
            .sum();
        let etag = {
            let part_count = filtered_content.len();

            // Parts created by UploadPartCopy carry no digest; they simply
            // don't contribute bytes to the combined ETag.
            let mut hashes_byte = Vec::new();
            for part in &filtered_content {
                if part.hash.len() == 32
                    && let Ok(bytes) = hex::decode(&part.hash) {
                        hashes_byte.extend(bytes);
                    }
            }

            let hash_md5 = md5::Md5::digest(&hashes_byte);
            let hash_md5 = hex::encode(hash_md5);

            Some(format!("{}-{}", hash_md5, part_count))
        };

        let delete_old_object_future = {
            let model = self
                .repo
                .get_object(&req.input.bucket, &req.input.key)
                .await;
            if let Ok(model) = model {
                let metadata: Metadata =
                    serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

                // Refcount-aware: shared blobs survive if something else
                // still references them.
                Some(self.release_blobs(metadata.item.into_iter().map(|v| v.id).collect()))
            } else {
                None
            }
        };

        let data = ObjectWrite {
            size,
            content_type: model.content_type,
            etag: etag.clone(),
            content: metadata_json,
            user_metadata: model.user_metadata,
        };

        self.repo
            .cas_put_object(
                req.input.bucket.clone(),
                req.input.key.clone(),
                data,
                condition,
            )
            .await?;

        // Delete the multipart upload state only after the object row is
        // committed, so a failed upsert doesn't orphan the uploaded parts.
        self.repo
            .delete_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        if let Some(delete_old_object_future) = delete_old_object_future {
            delete_old_object_future.await?;
        }

        // Parts not included in the request lose their reference; blobs
        // whose last reference disappeared are removed from the backend.
        let dangling_ids: Vec<String> = content
            .into_values()
            .flat_map(|multipart_upload_part| {
                multipart_upload_part.metadata_items.into_iter().map(|i| i.id)
            })
            .collect();
        self.release_blobs(dangling_ids).await?;

        let res = S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            e_tag: etag.map(ETag::Strong),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let model = self
            .repo
            .delete_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        let content = serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(model.content)
            .map_err(S3Error::internal_error)?;

        // Released parts may be shared with other objects; only unreferenced
        // blobs are removed from the backend.
        let part_ids: Vec<String> = content
            .into_values()
            .flat_map(|multipart_upload_part| {
                multipart_upload_part.metadata_items.into_iter().map(|i| i.id)
            })
            .collect();
        self.release_blobs(part_ids).await?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[instrument(skip(self), err)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = self
            .repo
            .get_object(&req.input.bucket, &req.input.key)
            .await?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

        let (mut offset, mut remain_length) = if let Some(range) = req.input.range {
            let r = range.check(model.size as u64)?;
            (r.start, r.end - r.start)
        } else {
            (0, model.size as u64)
        };

        let content_length = remain_length;

        let reader_futures = metadata.item.into_iter().filter_map(|item| {
            if remain_length == 0 {
                return None;
            }

            let item_size = item.size;

            if offset >= item_size {
                offset -= item_size;
                return None;
            }

            let local_offset = offset;
            let bytes_available = item_size - local_offset;
            let take_amount = std::cmp::min(bytes_available, remain_length);

            offset = 0;
            remain_length -= take_amount;

            // Items may be slices of larger shared blobs, so the read
            // position is the item's own blob offset plus the walk offset.
            let reader = self
                .backend
                .read(item.id, item.offset + local_offset, Some(take_amount));

            Some(reader)
        });
        let readers = futures::future::try_join_all(reader_futures)
            .await
            .map_err(S3Error::from)?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InternalError))?;

        let chain_readers = ChainReaders::from_vec(readers);

        let body = StreamingBlob::wrap(chain_readers);

        let object_metadata = json_to_metadata(&model.user_metadata);

        let res = S3Response::new(GetObjectOutput {
            content_type: model.content_type,
            content_length: Some(content_length as i64),
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
            metadata: object_metadata,
            body: Some(body),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let model = self
            .repo
            .get_object(&req.input.bucket, &req.input.key)
            .await?;

        let res = S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(model.size as i64),
            content_type: model.content_type,
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
            metadata: json_to_metadata(&model.user_metadata),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let model = self
            .repo
            .delete_object(&req.input.bucket, &req.input.key)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

        let ids: Vec<String> = metadata.item.iter().map(|item| item.id.clone()).collect();
        self.release_blobs(ids).await?;

        let res = S3Response::new(DeleteObjectOutput::default());

        Ok(res)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let keys: Vec<String> = req
            .input
            .delete
            .objects
            .iter()
            .map(|obj| obj.key.clone())
            .collect();

        let models = self.repo.delete_objects(&req.input.bucket, keys).await?;

        let metadatas: Vec<Metadata> = models
            .iter()
            .map(|model| {
                serde_json::from_value::<Metadata>(model.content.clone())
                    .map_err(S3Error::internal_error)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let ids: Vec<String> = metadatas
            .iter()
            .flat_map(|metadata| metadata.item.iter().map(|item| item.id.clone()))
            .collect();
        self.release_blobs(ids).await?;

        let quiet = req.input.delete.quiet.unwrap_or(false);

        let deleted = if quiet {
            None
        } else {
            let deleted_objects: Vec<DeletedObject> = models
                .iter()
                .map(|model| DeletedObject {
                    key: Some(model.id.clone()),
                    ..Default::default()
                })
                .collect();
            Some(deleted_objects)
        };

        let res = S3Response::new(DeleteObjectsOutput {
            deleted,
            errors: None,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let models = self
            .repo
            .list_objects(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.delimiter.clone(),
                req.input.marker.clone(),
                limit,
            )
            .await?;

        let (contents, common_prefix) = models.iter().fold(
            (Vec::<Object>::new(), Vec::<CommonPrefix>::new()),
            |mut result, model| {
                let common_prefix = {
                    let prefix = req.input.prefix.clone().unwrap_or_default();
                    let id = model.id.clone();

                    let id_without_prefix = id.strip_prefix(&prefix).unwrap_or(id.as_str());

                    if let Some(ref delimiter) = req.input.delimiter {
                        let sub_key = id_without_prefix.split_once(delimiter).map(|v| v.0);

                        sub_key.map(|sub_key| format!("{}{}{}", prefix, sub_key, delimiter,))
                    } else {
                        None
                    }
                };

                if let Some(common_prefix) = common_prefix {
                    result.1.push(CommonPrefix {
                        prefix: Some(common_prefix),
                    });
                } else {
                    result.0.push(Object {
                        key: Some(model.id.clone()),
                        size: Some(model.size.into()),
                        last_modified: Some(chrono_to_timestamp(model.last_modified)),
                        ..Default::default()
                    })
                }

                result
            },
        );

        let next_marker = if models.len() as u64 == limit {
            models.last().map(|model| model.id.clone())
        } else {
            None
        };

        let is_truncated = next_marker.is_some();

        let res = S3Response::new(ListObjectsOutput {
            contents: Some(contents),
            common_prefixes: Some(common_prefix),
            is_truncated: Some(is_truncated),
            marker: req.input.marker,
            next_marker,
            max_keys: Some(limit as i32),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let models = self
            .repo
            .list_objects(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.delimiter.clone(),
                // start-after only applies to the first page; once a
                // continuation token is present it takes precedence.
                req.input
                    .continuation_token
                    .clone()
                    .or(req.input.start_after.clone()),
                limit,
            )
            .await?;

        let (contents, common_prefix) = models.iter().fold(
            (Vec::<Object>::new(), Vec::<CommonPrefix>::new()),
            |mut result, model| {
                let common_prefix = {
                    let prefix = req.input.prefix.clone().unwrap_or_default();
                    let id = model.id.clone();

                    let id_without_prefix = id.strip_prefix(&prefix).unwrap_or(id.as_str());

                    if let Some(ref delimiter) = req.input.delimiter {
                        let sub_key = id_without_prefix.split_once(delimiter).map(|v| v.0);

                        sub_key.map(|sub_key| format!("{}{}{}", prefix, sub_key, delimiter,))
                    } else {
                        None
                    }
                };

                if let Some(common_prefix) = common_prefix {
                    result.1.push(CommonPrefix {
                        prefix: Some(common_prefix),
                    });
                } else {
                    result.0.push(Object {
                        key: Some(model.id.clone()),
                        size: Some(model.size.into()),
                        last_modified: Some(chrono_to_timestamp(model.last_modified)),
                        ..Default::default()
                    })
                }

                result
            },
        );

        let next_marker = if models.len() as u64 == limit {
            models.last().map(|model| model.id.clone())
        } else {
            None
        };

        let is_truncated = next_marker.is_some();

        let res = S3Response::new(ListObjectsV2Output {
            contents: Some(contents),
            common_prefixes: Some(common_prefix),
            is_truncated: Some(is_truncated),
            next_continuation_token: next_marker,
            key_count: Some(models.len() as i32),
            max_keys: Some(limit as i32),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn list_parts(
        &self,
        req: S3Request<ListPartsInput>,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let model = self
            .repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let content = serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(model.content)
            .map_err(S3Error::internal_error)?;

        // BTreeMap iteration is ascending by part number already.
        let mut parts: Vec<Part> = content
            .into_iter()
            .filter(|(num, _)| {
                req.input
                    .part_number_marker
                    .map(|marker| *num > marker)
                    .unwrap_or(true)
            })
            .map(|(num, p)| Part {
                part_number: Some(num),
                size: Some(p.metadata_items.iter().map(|i| i.size).sum::<u64>() as i64),
                e_tag: Some(ETag::Strong(p.hash)),
                ..Default::default()
            })
            .collect();

        let mut is_truncated = false;
        let mut next_part_number_marker: Option<i32> = None;

        if let Some(max_parts) = req.input.max_parts
            && parts.len() > max_parts as usize
        {
            parts.truncate(max_parts as usize);
            next_part_number_marker = parts.last().and_then(|p| p.part_number);
            is_truncated = true;
        }

        let res = S3Response::new(ListPartsOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(req.input.upload_id),
            parts: Some(parts),
            is_truncated: Some(is_truncated),
            max_parts: req.input.max_parts,
            part_number_marker: req.input.part_number_marker,
            next_part_number_marker,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;

        let models = self
            .repo
            .list_multipart_uploads(&req.input.bucket, req.input.prefix.as_deref())
            .await?;

        // Ordered by key ascending (repo query).
        let mut uploads: Vec<MultipartUpload> = models
            .into_iter()
            .map(|model| MultipartUpload {
                key: Some(model.object_id),
                upload_id: Some(model.upload_id),
                ..Default::default()
            })
            .collect();

        let mut is_truncated = false;

        if let Some(max_uploads) = req.input.max_uploads
            && uploads.len() > max_uploads as usize
        {
            uploads.truncate(max_uploads as usize);
            is_truncated = true;
        }

        let res = S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(req.input.bucket),
            uploads: Some(uploads),
            max_uploads: req.input.max_uploads,
            is_truncated: Some(is_truncated),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        // Access-point sources are not supported.
        let (src_bucket, src_key) = match &req.input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (&**bucket, &**key),
            CopySource::AccessPoint { .. } => {
                return Err(S3Error::new(S3ErrorCode::NotImplemented));
            }
        };

        let model = self.repo.get_object(src_bucket, src_key).await?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
        let source_size = model.size as u64;

        // Parse "bytes=start-end" (inclusive on both ends per S3 spec).
        let range = match req.input.copy_source_range.as_deref() {
            None => None,
            Some(r) => {
                let body = r
                    .trim()
                    .strip_prefix("bytes=")
                    .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument))?;

                if body.contains(',') {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument));
                }

                let (start_str, end_str) = body
                    .split_once('-')
                    .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument))?;

                let start = start_str
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
                let end = end_str
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;

                if end < start || end >= source_size {
                    return Err(S3Error::new(S3ErrorCode::InvalidArgument));
                }

                Some((start, end))
            }
        };

        let (read_offset, read_len) = match range {
            Some((start, end)) => (start, end - start + 1),
            None => (0, source_size),
        };

        if read_len == 0 {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }

        // The requested window [read_offset, read_offset + read_len) is
        // expressed as clipped slices of the source's existing blobs —
        // pure metadata surgery with reference acquisition, no data
        // movement. Every slice holds its own reference, so aborting this
        // upload can never damage the source object.
        let mut part_items: Vec<MetadataItem> = Vec::new();
        {
            let win_start = read_offset;
            let win_end = read_offset + read_len;

            let mut item_start = 0u64;
            for item in &metadata.item {
                if item_start >= win_end {
                    break;
                }

                let item_end = item_start + item.size;
                let overlap_start = item_start.max(win_start);
                let overlap_end = item_end.min(win_end);

                if overlap_start < overlap_end {
                    part_items.push(MetadataItem {
                        id: item.id.clone(),
                        offset: item.offset + (overlap_start - item_start),
                        size: overlap_end - overlap_start,
                    });
                }

                item_start = item_end;
            }

            // The source metadata must fully cover the requested window.
            let covered: u64 = part_items.iter().map(|v| v.size).sum();
            if covered != read_len {
                return Err(S3Error::new(S3ErrorCode::InternalError));
            }
        }

        // Every emitted slice takes its own reference on the shared blobs.
        let acquire_items: Vec<(String, u64)> = part_items
            .iter()
            .map(|v| (v.id.clone(), v.size))
            .collect();
        self.repo.acquire_blob_refs(&acquire_items).await?;

        // Compute the slice digest by streaming the shared bytes back once.
        // This costs a read pass through the backend per copied part but
        // keeps real ETags on every part and preserves the combined
        // multipart ETag algorithm. Each slice is already a (blob, offset,
        // length) triple, so one ranged backend read per slice suffices.
        let part_hash = {
            let reader_futures = part_items
                .iter()
                .map(|item| self.backend.read(item.id.clone(), item.offset, Some(item.size)));

            let readers = futures::future::try_join_all(reader_futures)
                .await
                .map_err(S3Error::from)?
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| S3Error::new(S3ErrorCode::InternalError))?;

            let hasher_md5 = Arc::new(Mutex::new(md5::Md5::new()));
            {
                // Hashed in fixed-size scratch buffers so the whole part is
                // never held in memory at once.
                let chain_readers = ChainReaders::from_vec(readers);
                let mut reader_with_hasher =
                    ReaderWithHasher::new(chain_readers, hasher_md5.clone());

                let mut scratch = vec![0u8; 64 * 1024];
                loop {
                    let n = reader_with_hasher
                        .read(&mut scratch)
                        .await
                        .map_err(S3Error::internal_error)?;
                    if n == 0 {
                        break;
                    }
                }
            }

            let hash_md5 = hasher_md5
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .finalize_reset();

            hex::encode(hash_md5)
        };

        let multipart_upload_part = MultipartUploadPart {
            hash: part_hash.clone(),
            metadata_items: part_items,
        };

        self.repo
            .cas_update_multipart_content(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                |content| {
                    let mut parts: BTreeMap<i32, MultipartUploadPart> =
                        serde_json::from_value(content.take())
                            .map_err(S3Error::internal_error)?;

                    parts.insert(req.input.part_number, multipart_upload_part.clone());

                    *content = serde_json::to_value(parts).map_err(S3Error::internal_error)?;

                    Ok(())
                },
            )
            .await?;

        let res = S3Response::new(UploadPartCopyOutput {
            copy_part_result: Some(CopyPartResult {
                e_tag: Some(ETag::Strong(part_hash)),
                last_modified: Some(chrono_to_timestamp(chrono::Local::now().to_utc())),
                ..Default::default()
            }),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn get_bucket_acl(
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
    async fn put_bucket_acl(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        // Existence-checked, otherwise accepted and ignored: this backend
        // has no ACL enforcement, but clients probing ACLs must not error.
        self.repo.get_bucket(&req.input.bucket).await?;

        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    #[instrument(skip(self), err)]
    async fn get_object_acl(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        self.repo.get_object(&req.input.bucket, &req.input.key).await?;

        Ok(S3Response::new(GetObjectAclOutput {
            owner: Some(canned_owner()),
            grants: Some(vec![full_control_grant()]),
            ..Default::default()
        }))
    }

    #[instrument(skip(self), err)]
    async fn put_object_acl(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.repo.get_object(&req.input.bucket, &req.input.key).await?;

        Ok(S3Response::new(PutObjectAclOutput::default()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metadata {
    item: Vec<MetadataItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataItem {
    id: String,
    // Start of this item's data within the backend blob. Zero for objects
    // that own whole blobs; non-zero when an item is a shared slice of a
    // larger blob (e.g. produced by a ranged upload part copy).
    #[serde(default)]
    offset: u64,
    size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipartUploadPart {
    // Digest of the part's bytes; empty when unknown (parts created by
    // UploadPartCopy share existing blobs and skip re-reading them).
    hash: String,
    metadata_items: Vec<MetadataItem>,
}

fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();

    Timestamp::from(datetime)
}

/// The single synthetic owner every resource is reported to belong to.
fn canned_owner() -> Owner {
    Owner {
        display_name: Some("tele-s3".into()),
        id: Some("tele-s3".into()),
    }
}

/// Full control granted to the synthetic owner.
fn full_control_grant() -> Grant {
    Grant {
        grantee: Some(Grantee {
            display_name: Some("tele-s3".into()),
            email_address: None,
            id: Some("tele-s3".into()),
            type_: s3s::dto::Type::CANONICAL_USER.to_string().into(),
            uri: None,
        }),
        permission: Some(s3s::dto::Permission::FULL_CONTROL.to_string().into()),
    }
}

fn metadata_to_json(metadata: Option<s3s::dto::Metadata>) -> serde_json::Value {
    match metadata {
        Some(map) if !map.is_empty() => {
            serde_json::to_value(map).unwrap_or_else(|_| serde_json::json!({}))
        }
        _ => serde_json::json!({}),
    }
}

fn json_to_metadata(value: &serde_json::Value) -> Option<s3s::dto::Metadata> {
    if value.is_null() {
        return None;
    }

    match serde_json::from_value::<s3s::dto::Metadata>(value.clone()) {
        Ok(map) if !map.is_empty() => Some(map),
        _ => None,
    }
}

/// Translate If-Match / If-None-Match headers into a write precondition.
fn build_put_condition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<PutCondition> {
    if if_match.is_some() && if_none_match.is_some() {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument));
    }

    fn etag_value(e: &ETag) -> String {
        match e {
            ETag::Strong(v) | ETag::Weak(v) => v.clone(),
        }
    }

    Ok(match (if_match, if_none_match) {
        (Some(ETagCondition::ETag(e)), _) => PutCondition::IfMatch(etag_value(e)),
        (Some(ETagCondition::Any), _) => PutCondition::IfMatchAny,
        (_, Some(ETagCondition::Any)) => PutCondition::IfNoneMatchAny,
        (_, Some(ETagCondition::ETag(e))) => PutCondition::IfNoneMatch(etag_value(e)),
        _ => PutCondition::None,
    })
}

trait StreamingBlobExt {
    fn into_boxed_reader(self) -> BoxedAsyncReader;
}

impl StreamingBlobExt for StreamingBlob {
    fn into_boxed_reader(self) -> BoxedAsyncReader {
        let stream = self.map_err(std::io::Error::other).into_async_read();
        Box::pin(stream)
    }
}
