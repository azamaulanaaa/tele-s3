use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use base64::Engine;
use s3s::{
    S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        CommonPrefix, CopyObjectInput, CopyObjectOutput, CopyObjectResult, CopySource,
        DeleteMarkerEntry, DeleteObjectInput, DeleteObjectOutput, DeleteObjectTaggingInput,
        DeleteObjectTaggingOutput, DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag,
        Error, GetObjectAclInput, GetObjectAclOutput, GetObjectInput, GetObjectOutput,
        GetObjectTaggingInput, GetObjectTaggingOutput, HeadObjectInput, HeadObjectOutput,
        ListObjectVersionsInput, ListObjectVersionsOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, Object, ObjectVersion, PutObjectAclInput,
        PutObjectAclOutput, PutObjectInput, PutObjectOutput, PutObjectTaggingInput,
        PutObjectTaggingOutput, StreamingBlob,
    },
};
use tracing::instrument;

use super::TeleS3;
use super::helpers::{
    StreamingBlobExt, build_put_condition, canned_owner, check_conditional_get, checksums_to_json,
    chrono_to_timestamp, delete_marker_error, full_control_grant, json_to_checksum_fields,
    json_to_metadata, json_to_tag_set, metadata_to_json, tagging_header_to_json, tags_to_json,
    verify_checksums,
};
use super::repo::ObjectWrite;
use super::types::{Metadata, MetadataItem};
use crate::backend::{Backend, ChainReaders, IntegrityDigests, IntegrityReader};

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(self), err)]
    pub(crate) async fn put_object_inner(
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

        let checksums = checksums_to_json(
            req.input.checksum_crc32.clone(),
            req.input.checksum_crc32c.clone(),
            req.input.checksum_sha1.clone(),
            req.input.checksum_sha256.clone(),
        )?;

        let expected_crc32 = req.input.checksum_crc32.clone();
        let expected_crc32c = req.input.checksum_crc32c.clone();
        let expected_sha1 = req.input.checksum_sha1.clone();
        let expected_sha256 = req.input.checksum_sha256.clone();

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        let (id, hash_md5, computed_crc32, computed_crc32c, computed_sha1, computed_sha256) = {
            let state = Arc::new(Mutex::new(IntegrityDigests::default()));

            let id = if size > 0 {
                let reader_with_hasher = Box::pin(IntegrityReader::new(reader, state.clone()));

                let id = self
                    .backend
                    .write(size, reader_with_hasher)
                    .await
                    .map_err(S3Error::from)?;
                Some(id)
            } else {
                None
            };

            let mut guard = state
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
            let hash_md5 = guard.finalize_md5_reset();
            let computed = (
                guard.finalize_crc32(),
                guard.finalize_crc32c(),
                guard.finalize_sha1(),
                guard.finalize_sha256(),
            );
            drop(guard);

            (id, hash_md5, computed.0, computed.1, computed.2, computed.3)
        };

        // The backend blob now exists; register it before any fallible
        // validation so a BadDigest cleanup can release it (mirrors the
        // Content-MD5 path below). Until cas_put_object links it, this
        // reference is owned solely by the incoming write.
        if let Some(ref id) = id {
            self.repo.register_new_blob(id.clone(), size).await?;
        }

        // Verify streaming checksums before the blob is linked. A mismatch
        // means corrupt transport: drop the backend blob and fail fast so
        // archives never store bad bytes under a good checksum.
        if let Err(e) = verify_checksums(
            computed_crc32,
            computed_crc32c,
            &computed_sha1,
            &computed_sha256,
            expected_crc32.as_deref(),
            expected_crc32c.as_deref(),
            expected_sha1.as_deref(),
            expected_sha256.as_deref(),
        ) {
            if let Some(id) = id {
                self.release_blobs(vec![id]).await?;
            }
            return Err(e);
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

        // For versioned buckets we keep old versions; don't release their blobs.
        let versioning_status = self.repo.get_bucket_versioning(&req.input.bucket).await?;
        let is_versioned = versioning_status.is_some();

        let delete_old_future = if is_versioned {
            None
        } else {
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

                Some(self.release_blobs(metadata.item.into_iter().map(|v| v.id).collect()))
            } else {
                None
            }
        };

        let tags = tagging_header_to_json(req.input.tagging.as_deref())?;

        let data = ObjectWrite {
            size,
            content_type: req.input.content_type.take(),
            etag: etag.clone(),
            content: content_json,
            user_metadata: metadata_to_json(req.input.metadata.take()),
            checksums: checksums.clone(),
            tags,
        };

        let version_id = {
            let bucket = req.input.bucket.clone();
            let key = req.input.key.clone();
            let result = self.repo.cas_put_object(bucket, key, data, condition).await;

            match result {
                Ok(vid) => vid,
                Err(err) => {
                    if let Some(id) = id {
                        let _ = self.release_blobs(vec![id]).await;
                    }
                    return Err(err);
                }
            }
        };

        if let Some(delete_old_future) = delete_old_future {
            delete_old_future.await?;
        }

        let (checksum_crc32, checksum_crc32c, checksum_sha1, checksum_sha256) =
            json_to_checksum_fields(&checksums);

        let response_version_id = if is_versioned { Some(version_id) } else { None };

        let res = S3Response::new(PutObjectOutput {
            e_tag: etag.map(ETag::Strong),
            size: Some(size as i64),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn copy_object_inner(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        // Access-point sources are not supported.
        let (src_bucket, src_key, src_version_id) = match &req.input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
                ..
            } => (&**bucket, &**key, version_id.as_deref()),
            CopySource::AccessPoint { .. } => {
                return Err(S3Error::new(S3ErrorCode::NotImplemented));
            }
        };

        let model = if let Some(vid) = src_version_id {
            self.repo
                .get_object_version(src_bucket, src_key, vid)
                .await?
        } else {
            self.repo.get_object(src_bucket, src_key).await?
        };

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

        let versioning_status = self.repo.get_bucket_versioning(&req.input.bucket).await?;
        let is_versioned = versioning_status.is_some();

        let delete_old_future = if is_versioned {
            None
        } else if let Ok(old) = self
            .repo
            .get_object(&req.input.bucket, &req.input.key)
            .await
        {
            let old_metadata: Metadata =
                serde_json::from_value(old.content).map_err(S3Error::internal_error)?;
            Some(self.release_blobs(old_metadata.item.into_iter().map(|v| v.id).collect()))
        } else {
            None
        };

        let is_replace = req
            .input
            .metadata_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == "REPLACE");

        let user_metadata = if is_replace {
            metadata_to_json(req.input.metadata.clone())
        } else {
            // AWS MetadataDirective defaults to COPY; carry source metadata.
            model.user_metadata.clone()
        };

        // Content-Type follows the same directive semantics in S3; when
        // REPLACE is specified the request value (if any) takes effect,
        // otherwise the source is preserved.
        let content_type = if is_replace {
            req.input
                .content_type
                .clone()
                .or(model.content_type.clone())
        } else {
            model.content_type.clone()
        };

        let checksums = if is_replace {
            // New metadata implies new object semantics; do not carry
            // stale checksums unless the caller also supplies them
            // (CopyObject does not carry checksum headers, so clear).
            serde_json::json!({})
        } else {
            // The bytes are identical on a metadata-level copy, so the
            // source checksums remain valid for the destination.
            model.checksums.clone()
        };

        // TaggingDirective defaults to COPY; REPLACE takes x-amz-tagging.
        let is_tag_replace = req
            .input
            .tagging_directive
            .as_ref()
            .is_some_and(|d| d.as_str() == "REPLACE");
        let tags = if is_tag_replace {
            tagging_header_to_json(req.input.tagging.as_deref())?
        } else {
            model.tags.clone()
        };

        let data = ObjectWrite {
            size,
            content_type,
            etag: model.etag.clone(),
            content: content_json,
            user_metadata,
            checksums,
            tags,
        };

        let version_id = self
            .repo
            .upsert_object(req.input.bucket.clone(), req.input.key.clone(), data)
            .await?;

        if let Some(delete_old_future) = delete_old_future {
            delete_old_future.await?;
        }

        let response_version_id = if is_versioned { Some(version_id) } else { None };

        let res = S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: model.etag.map(ETag::Strong),
                last_modified: Some(chrono_to_timestamp(model.last_modified)),
                ..Default::default()
            }),
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_object_inner(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = if let Some(vid) = req.input.version_id.as_deref() {
            let model = self
                .repo
                .get_object_version(&req.input.bucket, &req.input.key, vid)
                .await?;
            // Addressing a delete marker by version is MethodNotAllowed (405).
            if model.is_delete_marker {
                return Err(delete_marker_error(
                    &model.version_id,
                    S3ErrorCode::MethodNotAllowed,
                ));
            }
            model
        } else {
            match self.repo.get_object(&req.input.bucket, &req.input.key).await {
                Ok(model) => model,
                Err(err) if *err.code() == S3ErrorCode::NoSuchKey => {
                    // A delete-marker latest reads as NoSuchKey, but S3 still
                    // reports which marker hides the key.
                    if let Some(marker) = self
                        .repo
                        .get_latest_raw(&req.input.bucket, &req.input.key)
                        .await?
                        && marker.is_delete_marker
                    {
                        return Err(delete_marker_error(
                            &marker.version_id,
                            S3ErrorCode::NoSuchKey,
                        ));
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        };

        // Conditional GET checks (If-Match, If-None-Match, If-Modified-Since, If-Unmodified-Since)
        check_conditional_get(
            &model,
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
            req.input.if_unmodified_since.as_ref(),
        )?;

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
        let (checksum_crc32, checksum_crc32c, checksum_sha1, checksum_sha256) =
            json_to_checksum_fields(&model.checksums);

        // VersionId header: return it if bucket is versioned
        let versioning = self
            .repo
            .get_bucket_versioning(&req.input.bucket)
            .await
            .unwrap_or(None);
        let response_version_id = if versioning.is_some() {
            Some(model.version_id.clone())
        } else {
            None
        };

        let res = S3Response::new(GetObjectOutput {
            content_type: model.content_type,
            content_length: Some(content_length as i64),
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
            metadata: object_metadata,
            body: Some(body),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn head_object_inner(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let model = if let Some(vid) = req.input.version_id.as_deref() {
            let model = self
                .repo
                .get_object_version(&req.input.bucket, &req.input.key, vid)
                .await?;
            // Addressing a delete marker by version is MethodNotAllowed (405).
            if model.is_delete_marker {
                return Err(delete_marker_error(
                    &model.version_id,
                    S3ErrorCode::MethodNotAllowed,
                ));
            }
            model
        } else {
            match self.repo.get_object(&req.input.bucket, &req.input.key).await {
                Ok(model) => model,
                Err(err) if *err.code() == S3ErrorCode::NoSuchKey => {
                    // A delete-marker latest reads as NoSuchKey, but S3 still
                    // reports which marker hides the key.
                    if let Some(marker) = self
                        .repo
                        .get_latest_raw(&req.input.bucket, &req.input.key)
                        .await?
                        && marker.is_delete_marker
                    {
                        return Err(delete_marker_error(
                            &marker.version_id,
                            S3ErrorCode::NoSuchKey,
                        ));
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        };

        check_conditional_get(
            &model,
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
            req.input.if_unmodified_since.as_ref(),
        )?;

        let (checksum_crc32, checksum_crc32c, checksum_sha1, checksum_sha256) =
            json_to_checksum_fields(&model.checksums);

        let versioning = self
            .repo
            .get_bucket_versioning(&req.input.bucket)
            .await
            .unwrap_or(None);
        let response_version_id = if versioning.is_some() {
            Some(model.version_id.clone())
        } else {
            None
        };

        let res = S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(model.size as i64),
            content_type: model.content_type,
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
            metadata: json_to_metadata(&model.user_metadata),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn delete_object_inner(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let version_id_opt = req.input.version_id.clone();
        let (deleted_opt, is_marker) = self
            .repo
            .delete_object_versioned(&req.input.bucket, &req.input.key, version_id_opt.as_deref())
            .await?;

        // If we permanently deleted a version that had blobs, release them.
        if let Some(model) = deleted_opt.clone()
            && !model.is_delete_marker
            && !is_marker
        {
            // Permanent delete of a data version: release its blobs
            let metadata: Metadata =
                serde_json::from_value(model.content.clone()).map_err(S3Error::internal_error)?;
            let ids: Vec<String> = metadata.item.iter().map(|item| item.id.clone()).collect();
            self.release_blobs(ids).await?;
        }
        // If we created a delete marker, `deleted_opt` is the marker; no blobs to release.

        // For idempotent delete of non-existent key on non-versioned bucket, deleted_opt is None -> still return 204.
        // For versioned bucket, delete without versionId always creates a delete marker and returns it.
        let response_version_id = deleted_opt.as_ref().map(|m| m.version_id.clone());
        let delete_marker = if is_marker { Some(true) } else { None };

        let res = S3Response::new(DeleteObjectOutput {
            version_id: response_version_id,
            delete_marker,
            ..Default::default()
        });

        Ok(res)
    }

    pub(crate) async fn delete_objects_inner(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        // Version-aware bulk delete: each object may carry a versionId.
        // Per-key failures are reported in the `errors` list (S3 semantics);
        // only whole-request failures (e.g. blob GC) abort with Err.
        // One `Deleted` entry is emitted per requested object, in request
        // order, so duplicate keys and distinct versions are all reported.
        let mut deleted_objects: Vec<DeletedObject> = Vec::new();
        let mut all_blob_ids: Vec<String> = Vec::new();
        let mut errors: Vec<Error> = Vec::new();
        let bucket_versioning = self
            .repo
            .get_bucket_versioning(&req.input.bucket)
            .await
            .unwrap_or(None);
        let is_versioned = bucket_versioning.is_some();

        for obj in req.input.delete.objects.clone() {
            let vid = obj.version_id.clone();
            if is_versioned {
                match self
                    .repo
                    .delete_object_versioned(&req.input.bucket, &obj.key, vid.as_deref())
                    .await
                {
                    Ok((Some(m), is_marker)) => {
                        if !m.is_delete_marker
                            && !is_marker
                            && let Ok(md) = serde_json::from_value::<Metadata>(m.content.clone())
                        {
                            all_blob_ids.extend(md.item.into_iter().map(|v| v.id));
                        }
                        deleted_objects.push(DeletedObject {
                            key: Some(m.id.clone()),
                            version_id: Some(m.version_id.clone()),
                            ..Default::default()
                        });
                    }
                    Ok((None, _)) => {
                        // Idempotent delete of non-existent key: S3 still
                        // reports the key as deleted.
                        deleted_objects.push(DeletedObject {
                            key: Some(obj.key.clone()),
                            version_id: vid.clone(),
                            ..Default::default()
                        });
                    }
                    Err(e) if *e.code() == S3ErrorCode::NoSuchKey => {
                        // Permanent delete of a non-existent version is
                        // idempotent: report as deleted.
                        deleted_objects.push(DeletedObject {
                            key: Some(obj.key.clone()),
                            version_id: vid.clone(),
                            ..Default::default()
                        });
                    }
                    Err(e) => {
                        errors.push(delete_key_error(obj.key.clone(), vid.clone(), &e));
                    }
                }
            } else {
                // Non-versioned: pass a versionId through to the repo, where it
                // addresses the "null" version on match and is an idempotent
                // NoSuchKey otherwise. Never silently skip the delete.
                match self
                    .repo
                    .delete_object_versioned(&req.input.bucket, &obj.key, vid.as_deref())
                    .await
                {
                    Ok((Some(m), _)) => {
                        if let Ok(md) = serde_json::from_value::<Metadata>(m.content.clone()) {
                            all_blob_ids.extend(md.item.into_iter().map(|v| v.id));
                        }
                        deleted_objects.push(DeletedObject {
                            key: Some(m.id.clone()),
                            version_id: None,
                            ..Default::default()
                        });
                    }
                    Ok((None, _)) => {
                        // Idempotent delete of non-existent key.
                        deleted_objects.push(DeletedObject {
                            key: Some(obj.key.clone()),
                            version_id: obj.version_id.clone(),
                            ..Default::default()
                        });
                    }
                    Err(e) if *e.code() == S3ErrorCode::NoSuchKey => {
                        // Idempotent.
                        deleted_objects.push(DeletedObject {
                            key: Some(obj.key.clone()),
                            version_id: obj.version_id.clone(),
                            ..Default::default()
                        });
                    }
                    Err(e) => {
                        errors.push(delete_key_error(obj.key.clone(), vid.clone(), &e));
                    }
                }
            }
        }

        // Release blobs of permanently deleted data versions (best-effort
        // GC of shared blobs is scoped to these ids by release_blobs).
        if !all_blob_ids.is_empty() {
            self.release_blobs(all_blob_ids).await?;
        }

        let quiet = req.input.delete.quiet.unwrap_or(false);
        let deleted = if quiet { None } else { Some(deleted_objects) };

        let res = S3Response::new(DeleteObjectsOutput {
            deleted,
            errors: if errors.is_empty() {
                None
            } else {
                Some(errors)
            },
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn list_objects_inner(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        // Fetch one extra row to detect truncation without a false
        // positive when exactly `limit` items remain.
        let mut models = self
            .repo
            .list_objects(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.delimiter.clone(),
                req.input.marker.clone(),
                limit.saturating_add(1),
            )
            .await?;

        let is_truncated = models.len() as u64 > limit;
        if is_truncated {
            models.pop();
        }

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

        let next_marker = if is_truncated {
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
    pub(crate) async fn list_objects_v2_inner(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        // Fetch one extra row to detect truncation without a false
        // positive when exactly `limit` items remain.
        let mut models = self
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
                limit.saturating_add(1),
            )
            .await?;

        let is_truncated = models.len() as u64 > limit;
        if is_truncated {
            models.pop();
        }

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

        let next_marker = if is_truncated {
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
    pub(crate) async fn list_object_versions_inner(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;
        let max_keys = req.input.max_keys.unwrap_or(1000);
        // Fetch one extra row so truncation is detected from actual
        // remaining data instead of guessing from a full page.
        let fetch_max = max_keys.saturating_add(1);
        let (versions, delete_markers) = self
            .repo
            .list_object_versions(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.delimiter.clone(),
                req.input.key_marker.clone(),
                req.input.version_id_marker.clone(),
                Some(fetch_max),
            )
            .await?;

        // Recombine in global order (key asc, last-modified desc) so the
        // truncation cursor points at the true last returned entry,
        // regardless of how versions and delete markers interleave.
        let mut combined: Vec<super::repo::entity::object::Model> = versions
            .into_iter()
            .chain(delete_markers.into_iter())
            .collect();
        combined.sort_by(|a, b| {
            a.id.cmp(&b.id)
                .then_with(|| b.last_modified.cmp(&a.last_modified))
        });

        let is_truncated = combined.len() as i32 > max_keys && max_keys > 0;
        if is_truncated {
            combined.pop();
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            match combined.last() {
                Some(last) => (Some(last.id.clone()), Some(last.version_id.clone())),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        let mut versions: Vec<super::repo::entity::object::Model> = Vec::new();
        let mut delete_markers: Vec<super::repo::entity::object::Model> = Vec::new();
        for m in combined {
            if m.is_delete_marker {
                delete_markers.push(m);
            } else {
                versions.push(m);
            }
        }

        // Build common prefixes if delimiter present - similar to list_objects but for versions we need to deduplicate prefixes from all keys
        let delimiter = req.input.delimiter.clone();
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let mut common_prefixes_set = BTreeSet::new();
        let mut filtered_versions = Vec::new();
        let mut filtered_delete_markers = Vec::new();

        if let Some(del) = delimiter.clone() {
            for v in &versions {
                if let Some(key) = v.id.strip_prefix(&prefix)
                    && let Some((pre, _)) = key.split_once(&del)
                {
                    common_prefixes_set.insert(format!("{}{}{}", prefix, pre, del));
                    continue;
                }
                filtered_versions.push(v.clone());
            }
            for d in &delete_markers {
                if let Some(key) = d.id.strip_prefix(&prefix)
                    && let Some((pre, _)) = key.split_once(&del)
                {
                    common_prefixes_set.insert(format!("{}{}{}", prefix, pre, del));
                    continue;
                }
                filtered_delete_markers.push(d.clone());
            }
        } else {
            filtered_versions = versions;
            filtered_delete_markers = delete_markers;
        }

        let common_prefixes = if common_prefixes_set.is_empty() {
            None
        } else {
            Some(
                common_prefixes_set
                    .into_iter()
                    .map(|p| s3s::dto::CommonPrefix { prefix: Some(p) })
                    .collect(),
            )
        };

        let versions_out: Vec<ObjectVersion> = filtered_versions
            .into_iter()
            .map(|m| ObjectVersion {
                key: Some(m.id.clone()),
                version_id: Some(m.version_id.clone()),
                is_latest: Some(m.is_latest),
                last_modified: Some(chrono_to_timestamp(m.last_modified)),
                e_tag: m.etag.clone().map(ETag::Strong),
                size: Some(m.size as i64),
                storage_class: Some(s3s::dto::ObjectVersionStorageClass::from_static(
                    s3s::dto::ObjectVersionStorageClass::STANDARD,
                )),
                owner: Some(canned_owner()),
                ..Default::default()
            })
            .collect();

        let delete_markers_out: Vec<DeleteMarkerEntry> = filtered_delete_markers
            .into_iter()
            .map(|m| DeleteMarkerEntry {
                key: Some(m.id.clone()),
                version_id: Some(m.version_id.clone()),
                is_latest: Some(m.is_latest),
                last_modified: Some(chrono_to_timestamp(m.last_modified)),
                owner: Some(canned_owner()),
            })
            .collect();

        Ok(S3Response::new(ListObjectVersionsOutput {
            name: Some(req.input.bucket.clone()),
            prefix: req.input.prefix.clone(),
            key_marker: req.input.key_marker.clone(),
            version_id_marker: req.input.version_id_marker.clone(),
            max_keys: Some(max_keys),
            is_truncated: Some(is_truncated),
            next_key_marker,
            next_version_id_marker,
            versions: if versions_out.is_empty() {
                None
            } else {
                Some(versions_out)
            },
            delete_markers: if delete_markers_out.is_empty() {
                None
            } else {
                Some(delete_markers_out)
            },
            common_prefixes,
            delimiter: req.input.delimiter.clone(),
            ..Default::default()
        }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_object_acl_inner(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        if let Some(vid) = req.input.version_id.as_deref() {
            self.repo
                .get_object_version(&req.input.bucket, &req.input.key, vid)
                .await?;
        } else {
            self.repo
                .get_object(&req.input.bucket, &req.input.key)
                .await?;
        }
        Ok(S3Response::new(GetObjectAclOutput {
            owner: Some(canned_owner()),
            grants: Some(vec![full_control_grant()]),
            ..Default::default()
        }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn put_object_acl_inner(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        if let Some(vid) = req.input.version_id.as_deref() {
            self.repo
                .get_object_version(&req.input.bucket, &req.input.key, vid)
                .await?;
        } else {
            self.repo
                .get_object(&req.input.bucket, &req.input.key)
                .await?;
        }
        Ok(S3Response::new(PutObjectAclOutput::default()))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_object_tagging_inner(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let model = if let Some(vid) = req.input.version_id.as_deref() {
            self.repo
                .get_object_version(&req.input.bucket, &req.input.key, vid)
                .await?
        } else {
            self.repo
                .get_object(&req.input.bucket, &req.input.key)
                .await?
        };
        let version_id = {
            let vs = self
                .repo
                .get_bucket_versioning(&req.input.bucket)
                .await
                .unwrap_or(None);
            if vs.is_some() {
                Some(model.version_id.clone())
            } else {
                None
            }
        };
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set: json_to_tag_set(&model.tags),
            version_id,
        }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn put_object_tagging_inner(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let vid = req.input.version_id.clone();
        self.repo
            .set_object_tags_versioned(
                &req.input.bucket,
                &req.input.key,
                vid.as_deref(),
                tags_to_json(req.input.tagging),
            )
            .await?;
        // Without an explicit versionId the tagging applies to latest;
        // report its version on versioned buckets.
        let version_id = match vid {
            Some(v) => Some(v),
            None => {
                let versioned = self
                    .repo
                    .get_bucket_versioning(&req.input.bucket)
                    .await
                    .unwrap_or(None)
                    .is_some();
                if versioned {
                    self.repo
                        .latest_version_id(&req.input.bucket, &req.input.key)
                        .await?
                } else {
                    None
                }
            }
        };
        Ok(S3Response::new(PutObjectTaggingOutput { version_id }))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn delete_object_tagging_inner(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let vid = req.input.version_id.clone();
        self.repo
            .set_object_tags_versioned(
                &req.input.bucket,
                &req.input.key,
                vid.as_deref(),
                serde_json::json!([]),
            )
            .await?;
        // Without an explicit versionId the delete applies to latest;
        // report its version on versioned buckets.
        let version_id = match vid {
            Some(v) => Some(v),
            None => {
                let versioned = self
                    .repo
                    .get_bucket_versioning(&req.input.bucket)
                    .await
                    .unwrap_or(None)
                    .is_some();
                if versioned {
                    self.repo
                        .latest_version_id(&req.input.bucket, &req.input.key)
                        .await?
                } else {
                    None
                }
            }
        };
        Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }))
    }
}

/// Map a per-key bulk-delete failure to an S3 `DeleteObjects` error entry.
fn delete_key_error(key: String, version_id: Option<String>, err: &S3Error) -> Error {
    Error {
        code: Some(err.code().as_str().to_owned()),
        key: Some(key),
        message: err.message().map(str::to_owned),
        version_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::Memory;
    use crate::s3::TeleS3;

    async fn test_service() -> TeleS3<Memory<1, 1>> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(Memory::default(), db).await.expect("init")
    }

    fn get_req(bucket: &str, key: &str, version_id: Option<String>) -> S3Request<GetObjectInput> {
        S3Request {
            input: GetObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn head_req(bucket: &str, key: &str, version_id: Option<String>) -> S3Request<HeadObjectInput> {
        S3Request {
            input: HeadObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id,
                ..Default::default()
            },
            method: http::Method::HEAD,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn versioned_bucket_with_marker(svc: &TeleS3<Memory<1, 1>>) -> String {
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.repo
            .put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");
        svc.repo
            .cas_put_object(
                "b".into(),
                "k".into(),
                ObjectWrite {
                    size: 0,
                    content_type: None,
                    etag: Some("e1".into()),
                    content: serde_json::json!({"item": []}),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    tags: serde_json::json!([]),
                },
                super::super::repo::PutCondition::None,
            )
            .await
            .expect("put object");

        let (marker, is_marker) = svc
            .repo
            .delete_object_versioned("b", "k", None)
            .await
            .expect("delete object");
        assert!(is_marker, "delete should create a marker");
        marker.expect("marker model").version_id
    }

    #[tokio::test]
    async fn get_on_delete_marker_reports_marker_headers() {
        let svc = test_service().await;
        let marker_vid = versioned_bucket_with_marker(&svc).await;

        let err = svc
            .get_object_inner(get_req("b", "k", None))
            .await
            .expect_err("get on marker should fail");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);

        // s3s renders S3Error headers on the wire; take_headers is
        // crate-private to s3s, so assert through the Debug rendering.
        let debug = format!("{err:?}");
        assert!(
            debug.contains("x-amz-delete-marker"),
            "missing delete-marker header: {debug}"
        );
        assert!(
            debug.contains(&marker_vid),
            "missing marker version id: {debug}"
        );
    }

    #[tokio::test]
    async fn head_on_delete_marker_reports_marker_headers() {
        let svc = test_service().await;
        let marker_vid = versioned_bucket_with_marker(&svc).await;

        let err = svc
            .head_object_inner(head_req("b", "k", None))
            .await
            .expect_err("head on marker should fail");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);

        let debug = format!("{err:?}");
        assert!(
            debug.contains("x-amz-delete-marker"),
            "missing delete-marker header: {debug}"
        );
        assert!(
            debug.contains(&marker_vid),
            "missing marker version id: {debug}"
        );
    }

    #[tokio::test]
    async fn version_addressed_marker_read_reports_marker_headers() {
        let svc = test_service().await;
        let marker_vid = versioned_bucket_with_marker(&svc).await;

        let err = svc
            .get_object_inner(get_req("b", "k", Some(marker_vid.clone())))
            .await
            .expect_err("versioned marker read should fail");
        assert_eq!(*err.code(), S3ErrorCode::MethodNotAllowed);

        let debug = format!("{err:?}");
        assert!(
            debug.contains("x-amz-delete-marker"),
            "missing delete-marker header: {debug}"
        );
        assert!(
            debug.contains(&marker_vid),
            "missing marker version id: {debug}"
        );
    }

    #[tokio::test]
    async fn missing_key_has_no_delete_marker_headers() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let err = svc
            .get_object_inner(get_req("b", "missing", None))
            .await
            .expect_err("get on missing key should fail");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);

        let debug = format!("{err:?}");
        assert!(
            !debug.contains("x-amz-delete-marker"),
            "plain missing key must not claim a marker: {debug}"
        );
    }
}
