use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use base64::Engine;
use digest::Digest;
use futures::io::AsyncReadExt;
use s3s::{
    S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, CompleteMultipartUploadInput,
        CompleteMultipartUploadOutput, CopyPartResult, CopySource, CreateMultipartUploadInput,
        CreateMultipartUploadOutput, ETag, ListMultipartUploadsInput, ListMultipartUploadsOutput,
        ListPartsInput, ListPartsOutput, MultipartUpload, Part, UploadPartCopyInput,
        UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
    },
};
use tracing::instrument;

use super::TeleS3;
use super::helpers::{
    ExpectedChecksums, StreamingBlobExt, build_put_condition, chrono_to_timestamp,
    metadata_to_json, tagging_header_to_json, verify_checksums,
};
use super::objects::{checked_content_length, read_outcome_to_reader};
use super::repo::{MultipartCompletion, MultipartUploadStateUpdate, ObjectWrite};
use super::types::{Metadata, MetadataItem, MultipartUploadPart};
use crate::backend::{
    Backend, ChainReaders, IntegrityDigests, IntegrityReader, ReaderWithHasher, new_publish_id,
};

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(self), err)]
    pub(crate) async fn create_multipart_upload_inner(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;

        let upload_id = uuid::Uuid::new_v4().to_string();

        let content = BTreeMap::<i32, MultipartUploadPart>::new();
        let content_json = serde_json::to_value(&content).map_err(S3Error::internal_error)?;
        let tags = tagging_header_to_json(req.input.tagging.as_deref())?;

        self.repo
            .upsert_multipart_upload_state(MultipartUploadStateUpdate {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id: upload_id.clone(),
                content_type: req.input.content_type,
                user_metadata: metadata_to_json(req.input.metadata.clone()),
                tags,
                content: content_json,
            })
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
    pub(crate) async fn upload_part_inner(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let _model = self
            .repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let size = checked_content_length(req.input.content_length)?;

        if size == 0 {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }

        let expected_checksums = ExpectedChecksums::new(
            req.input.checksum_crc32.clone(),
            req.input.checksum_crc32c.clone(),
            req.input.checksum_sha1.clone(),
            req.input.checksum_sha256.clone(),
        );

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        // Captured outside the block so the failure paths below can clear it
        // and the CAS update can resolve it.
        let publish_id: String;

        let (id, hash_md5, computed_crc32, computed_crc32c, computed_sha1, computed_sha256) = {
            let state = Arc::new(Mutex::new(IntegrityDigests::default()));

            let reader_with_hasher = Box::pin(IntegrityReader::new(reader, state.clone()));

            // As in `put_object_inner`: the intent is durable before the
            // backend is called, and only a confirmed publish clears it.
            let pid = new_publish_id();
            self.repo
                .record_publish_intent(
                    pid.clone(),
                    req.input.bucket.clone(),
                    req.input.key.clone(),
                    size,
                )
                .await?;

            publish_id = pid.clone();

            let id = self
                .backend
                .write(pid.clone(), size, reader_with_hasher)
                .await
                .map_err(S3Error::from)?;

            // Do NOT resolve the intent here: resolution commits with the
            // multipart content update below.

            let mut guard = state
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
            let hash_md5 = guard.finalize_md5_reset().clone();
            let computed = (
                guard.finalize_crc32(),
                guard.finalize_crc32c(),
                guard.finalize_sha1(),
                guard.finalize_sha256(),
            );
            drop(guard);

            (id, hash_md5, computed.0, computed.1, computed.2, computed.3)
        };

        // The part blob is owned by the multipart upload state until the
        // upload completes (ownership moves to the object) or aborts.
        //
        // Registration stays a separate transaction here (unlike
        // `PutObject`, which publishes blob + object together): the state
        // update that links this blob is a bounded CAS retry loop
        // (`cas_update_multipart_content`). The blob row exists before the
        // state row, so a crash between the two leaves an unreferenced row
        // that the start-up blob reconciler reclaims.
        if let Err(e) = self.repo.register_new_blob(id.clone(), size).await {
            // The backend blob was written but its bookkeeping row never
            // landed; drop the bytes so they do not leak, then propagate
            // the original error.
            let _ = self.release_blobs(vec![id.clone()]).await;
            let _ = self.discard_backend_blobs(vec![id]).await;
            return Err(e);
        }

        // `verify_checksums` distinguishes an undecodable client checksum
        // (`InvalidDigest`) from a genuine mismatch (`BadDigest`); both are
        // propagated as-is so the client learns which one happened.
        if let Err(e) = verify_checksums(
            computed_crc32,
            computed_crc32c,
            &computed_sha1,
            &computed_sha256,
            &expected_checksums,
        ) {
            self.release_blobs(vec![id]).await?;
            let _ = self
                .repo
                .clear_publish_intents(std::slice::from_ref(&publish_id))
                .await;
            return Err(e);
        }

        if let Some(expected) = req.input.content_md5 {
            let expected_digest = if let Ok(v) = base64::prelude::BASE64_STANDARD.decode(expected.trim()) { v } else {
                self.release_blobs(vec![id]).await?;
                let _ = self
                    .repo
                    .clear_publish_intents(std::slice::from_ref(&publish_id))
                    .await;
                return Err(S3Error::new(S3ErrorCode::InvalidDigest));
            };

            if expected_digest[..] != hash_md5[..] {
                self.release_blobs(vec![id]).await?;
                let _ = self
                    .repo
                    .clear_publish_intents(std::slice::from_ref(&publish_id))
                    .await;
                return Err(S3Error::new(S3ErrorCode::BadDigest));
            }
        }

        let blob_id = id.clone();

        let multipart_upload_part = MultipartUploadPart {
            hash: hex::encode(hash_md5),
            metadata_items: vec![MetadataItem {
                id,
                offset: 0,
                size,
            }],
        };

        let replaced = match self
            .repo
            .set_multipart_part(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                req.input.part_number,
                multipart_upload_part.clone(),
                Some((&publish_id, &blob_id)),
            )
            .await
        {
            Ok(replaced) => replaced,
            Err(e) => {
                // The blob was registered but never linked into the upload
                // state; release it so it does not leak.
                self.release_blobs(vec![blob_id.clone()]).await?;
                // The release settled this publish, so no intent needs to
                // remain.
                let _ = self
                    .repo
                    .clear_publish_intents(std::slice::from_ref(&publish_id))
                    .await;
                return Err(e);
            }
        };

        // The part row now owns the new blob. Remove any blob the replaced
        // part was the last holder of.
        self.release_blobs(replaced).await?;
        let res = S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(multipart_upload_part.hash)),
            // Echo verified checksums back so SDKs can confirm the part
            // landed intact; unverified (absent) checksums stay absent.
            checksum_crc32: expected_checksums.crc32,
            checksum_crc32c: expected_checksums.crc32c,
            checksum_sha1: expected_checksums.sha1,
            checksum_sha256: expected_checksums.sha256,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn complete_multipart_upload_inner(
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

        // Keep the raw content so the commit below can prove the state did
        // not change while the object metadata was built.
        let state_content = model.content.clone();
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

            // S3's combined ETag: the MD5 of the concatenated per-part
            // digests. Every part carries a real MD5 (including one written by
            // UploadPartCopy, which re-reads the copied bytes to hash them), so
            // all of them contribute. The length/hex test below is defensive
            // only: `MultipartUploadPart::hash` has no serde default, so a part
            // stored without a digest fails deserialization earlier.
            let mut hashes_byte = Vec::new();
            for part in &filtered_content {
                if part.hash.len() == 32
                    && let Ok(bytes) = hex::decode(&part.hash)
                {
                    hashes_byte.extend(bytes);
                }
            }

            let hash_md5 = md5::Md5::digest(&hashes_byte);
            let hash_md5 = hex::encode(hash_md5);

            Some(format!("{hash_md5}-{part_count}"))
        };

        let versioning_status = self.repo.get_bucket_versioning(&req.input.bucket).await?;
        let is_versioned = versioning_status.is_some();

        // A multipart upload usually completes onto a key that does not exist
        // yet, so absence is a silent no-op; a failed read must not be.
        let previous_object = self
            .replaced_object(&req.input.bucket, &req.input.key)
            .await?;

        // References this completion retires: the parts left out of the
        // object and, for an unversioned bucket, the object it replaces.
        // They are released in the same transaction as the object write.
        let mut release_ids: Vec<String> = if is_versioned {
            Vec::new()
        } else if let Some(previous_object) = previous_object {
            let metadata: Metadata =
                serde_json::from_value(previous_object.content).map_err(S3Error::internal_error)?;
            metadata.item.into_iter().map(|v| v.id).collect()
        } else {
            Vec::new()
        };
        release_ids.extend(content.into_values().flat_map(|multipart_upload_part| {
            multipart_upload_part
                .metadata_items
                .into_iter()
                .map(|i| i.id)
        }));

        let data = ObjectWrite {
            size,
            content_type: model.content_type,
            etag: etag.clone(),
            content: metadata_json,
            user_metadata: model.user_metadata,
            checksums: serde_json::json!({}),
            // Pre-tag uploads store NULL; they had no tag support.
            tags: model.tags.clone().unwrap_or(serde_json::json!([])),
        };

        // The object row, the upload-state row and the retired references
        // commit together. A failure at any step rolls all of them back, so
        // a completed object can never share its part references with a
        // state row that still owns them, and no old reference is lost.
        let (version_id, released) = self
            .repo
            .complete_multipart_upload(MultipartCompletion {
                bucket: req.input.bucket.clone(),
                key: req.input.key.clone(),
                upload_id: req.input.upload_id.clone(),
                expected_content: state_content,
                data,
                condition,
                release_ids,
            })
            .await?;

        // The bookkeeping is final. Remove the backend bytes of blobs whose
        // count reached zero; a backend failure leaves their rows for a
        // later retry.
        self.release_blobs(released).await?;

        let response_version_id = if is_versioned { Some(version_id) } else { None };

        let res = S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            e_tag: etag.map(ETag::Strong),
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn abort_multipart_upload_inner(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        // The state row and its references change together. If the release
        // fails, the state stays, so the upload can be aborted again instead
        // of leaving ownerless references behind.
        let released = self
            .repo
            .delete_multipart_upload_state_releasing_blobs(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                |content| {
                    let content = serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(
                        content.clone(),
                    )
                    .map_err(S3Error::internal_error)?;

                    Ok(content
                        .into_values()
                        .flat_map(|multipart_upload_part| {
                            multipart_upload_part
                                .metadata_items
                                .into_iter()
                                .map(|i| i.id)
                        })
                        .collect())
                },
            )
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        // Released parts may be shared with other objects; only
        // unreferenced blobs are removed from the backend.
        self.release_blobs(released).await?;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn list_parts_inner(
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
                    .is_none_or(|marker| *num > marker)
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

        if let Some(max_parts) = req.input.max_parts {
            let max_parts = usize::try_from(max_parts)
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
            if parts.len() > max_parts {
                parts.truncate(max_parts);
                next_part_number_marker = parts.last().and_then(|p| p.part_number);
                is_truncated = true;
            }
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
    pub(crate) async fn list_multipart_uploads_inner(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.repo.get_bucket(&req.input.bucket).await?;

        let (models, is_truncated) = self
            .repo
            .list_multipart_uploads(
                &req.input.bucket,
                req.input.prefix.as_deref(),
                req.input.key_marker.as_deref(),
                req.input.upload_id_marker.as_deref(),
                req.input.max_uploads,
            )
            .await?;

        // Ordered by (key, upload_id) ascending (repo query).
        let uploads: Vec<MultipartUpload> = models
            .iter()
            .map(|model| MultipartUpload {
                key: Some(model.object_id.clone()),
                upload_id: Some(model.upload_id.clone()),
                ..Default::default()
            })
            .collect();

        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            models
                .last()
                .map(|m| (m.object_id.clone(), m.upload_id.clone()))
        } else {
            None
        }
        .map_or((None, None), |(k, u)| (Some(k), Some(u)));

        let res = S3Response::new(ListMultipartUploadsOutput {
            bucket: Some(req.input.bucket),
            uploads: Some(uploads),
            key_marker: req.input.key_marker,
            upload_id_marker: req.input.upload_id_marker,
            next_key_marker,
            next_upload_id_marker,
            max_uploads: req.input.max_uploads,
            is_truncated: Some(is_truncated),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn upload_part_copy_inner(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let (src_bucket, src_key, src_version_id) = match &req.input.copy_source {
            CopySource::Bucket {
                bucket,
                key,
                version_id,
                ..
            } => (&**bucket, &**key, version_id.as_deref()),
            CopySource::AccessPoint { .. } | CopySource::Outpost { .. } => {
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

        // A versioned source may address a delete marker; there are no
        // bytes to copy from it.
        if model.is_delete_marker {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey));
        }

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
        let source_size = u64::from(model.size);

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
        let acquire_items: Vec<(String, u64)> =
            part_items.iter().map(|v| (v.id.clone(), v.size)).collect();
        self.repo.acquire_blob_refs(&acquire_items).await?;

        // Compute the slice digest by streaming the shared bytes back once.
        // This costs a read pass through the backend per copied part but
        // keeps real ETags on every part and preserves the combined
        // multipart ETag algorithm. Each slice is already a (blob, offset,
        // length) triple, so one ranged backend read per slice suffices.
        let part_hash_result: S3Result<String> = async {
            let reader_futures = part_items.iter().map(|item| {
                self.backend
                    .read(item.id.clone(), item.offset, Some(item.size))
            });

            let readers = futures::future::try_join_all(reader_futures)
                .await
                .map_err(S3Error::from)?
                .into_iter()
                .map(read_outcome_to_reader)
                .collect::<S3Result<Vec<_>>>()?;

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

            Ok(hex::encode(hash_md5))
        }
        .await;

        let part_hash = match part_hash_result {
            Ok(hash) => hash,
            Err(e) => {
                let ids: Vec<String> = acquire_items.iter().map(|(id, _)| id.clone()).collect();
                self.release_blobs(ids).await?;
                return Err(e);
            }
        };

        let multipart_upload_part = MultipartUploadPart {
            hash: part_hash.clone(),
            metadata_items: part_items,
        };

        let replaced = match self
            .repo
            .set_multipart_part(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                req.input.part_number,
                multipart_upload_part,
                None,
            )
            .await
        {
            Ok(replaced) => replaced,
            Err(e) => {
                let ids: Vec<String> = acquire_items.iter().map(|(id, _)| id.clone()).collect();
                self.release_blobs(ids).await?;
                return Err(e);
            }
        };
        // The part row now owns the new references. Remove any blob the
        // replaced part was the last holder of.
        self.release_blobs(replaced).await?;

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
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;
    use crate::backend::{Memory, ReadOutcome};
    use crate::s3::TeleS3;
    use sea_orm::{ConnectionTrait, EntityTrait};

    async fn test_service_with_upload() -> (TeleS3<Memory<2, 1>>, String) {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        let svc = TeleS3::init(Memory::default(), db).await.expect("init");
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        let upload_id = "u1".to_string();
        svc.repo
            .upsert_multipart_upload_state(MultipartUploadStateUpdate {
                bucket: "b".into(),
                key: "k".into(),
                upload_id: upload_id.clone(),
                content_type: None,
                user_metadata: serde_json::json!({}),
                tags: serde_json::json!([]),
                content: serde_json::json!({}),
            })
            .await
            .expect("create upload state");
        (svc, upload_id)
    }

    async fn intents(
        svc: &TeleS3<impl Backend>,
    ) -> Vec<crate::s3::repo::entity::publish_intent::Model> {
        crate::s3::repo::entity::publish_intent::Entity::find()
            .all(&svc.repo.db)
            .await
            .expect("load publish intents")
    }

    async fn blob_rows(svc: &TeleS3<impl Backend>) -> Vec<crate::s3::repo::entity::blob::Model> {
        crate::s3::repo::entity::blob::Entity::find()
            .all(&svc.repo.db)
            .await
            .expect("load blob rows")
    }

    /// Make every update of a publish intent fail, so a resolve failure is
    /// observable without a broken database connection.
    async fn refuse_publish_intent_updates(svc: &TeleS3<impl Backend>) {
        svc.repo
            .db
            .execute_unprepared(
                "CREATE TRIGGER refuse_publish_intent_update \
                 BEFORE UPDATE ON s3_publish_intent \
                 BEGIN SELECT RAISE(ABORT, 'intent update refused'); END",
            )
            .await
            .expect("create trigger");
    }

    fn list_parts_req(upload_id: &str, max_parts: Option<i32>) -> S3Request<ListPartsInput> {
        S3Request {
            input: ListPartsInput {
                bucket: "b".to_string(),
                key: "k".to_string(),
                upload_id: upload_id.to_string(),
                max_parts,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: "/b/k".parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn upload_part_req(upload_id: &str, content_length: Option<i64>) -> S3Request<UploadPartInput> {
        S3Request {
            input: UploadPartInput {
                bucket: "b".to_string(),
                key: "k".to_string(),
                upload_id: upload_id.to_string(),
                part_number: 1,
                content_length,
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: "/b/k".parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// A one-byte `UploadPart` carrying `checksum_crc32` as the client's
    /// `x-amz-checksum-crc32` value.
    fn upload_part_with_crc32_req(
        upload_id: &str,
        checksum_crc32: &str,
    ) -> S3Request<UploadPartInput> {
        let mut req = upload_part_req(upload_id, Some(1));
        req.input.body = Some(s3s::dto::StreamingBlob::from_bytes(
            bytes::Bytes::from_static(b"x"),
        ));
        req.input.checksum_crc32 = Some(checksum_crc32.to_string());
        req
    }

    fn list_uploads_req(
        max_uploads: Option<i32>,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
    ) -> S3Request<ListMultipartUploadsInput> {
        S3Request {
            input: ListMultipartUploadsInput {
                bucket: "b".to_string(),
                max_uploads,
                key_marker,
                upload_id_marker,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: "/b".parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn complete_upload_req(upload_id: &str) -> S3Request<CompleteMultipartUploadInput> {
        S3Request {
            input: CompleteMultipartUploadInput {
                bucket: "b".to_string(),
                key: "k".to_string(),
                upload_id: upload_id.to_string(),
                multipart_upload: Some(s3s::dto::CompletedMultipartUpload {
                    parts: Some(vec![s3s::dto::CompletedPart {
                        part_number: Some(1),
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            },
            method: http::Method::POST,
            uri: "/b/k".parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn upload_one_byte_part_req(upload_id: &str) -> S3Request<UploadPartInput> {
        let mut req = upload_part_req(upload_id, Some(1));
        req.input.body = Some(s3s::dto::StreamingBlob::from_bytes(
            bytes::Bytes::from_static(b"x"),
        ));
        req
    }

    fn abort_upload_req(upload_id: &str) -> S3Request<AbortMultipartUploadInput> {
        S3Request {
            input: AbortMultipartUploadInput {
                bucket: "b".to_string(),
                key: "k".to_string(),
                upload_id: upload_id.to_string(),
                ..Default::default()
            },
            method: http::Method::DELETE,
            uri: "/b/k".parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// Return the blob ids that one stored upload content owns.
    fn upload_blob_ids(content: &serde_json::Value) -> Vec<String> {
        let parts: BTreeMap<i32, MultipartUploadPart> =
            serde_json::from_value(content.clone()).expect("upload content");
        parts
            .into_values()
            .flat_map(|part| part.metadata_items.into_iter().map(|item| item.id))
            .collect()
    }

    /// Make every blob update fail, as a storage fault would.
    async fn fail_blob_updates<B: crate::backend::Backend>(svc: &TeleS3<B>) {
        svc.repo
            .db
            .execute_unprepared(
                "CREATE TRIGGER fail_blob_release BEFORE UPDATE ON s3_blob \
                 BEGIN SELECT RAISE(ABORT, 'injected blob release failure'); END",
            )
            .await
            .expect("create blob failure trigger");
    }

    async fn allow_blob_updates<B: crate::backend::Backend>(svc: &TeleS3<B>) {
        svc.repo
            .db
            .execute_unprepared("DROP TRIGGER fail_blob_release")
            .await
            .expect("drop blob failure trigger");
    }

    /// Make the upload-state row reject deletion, as a storage fault would.
    async fn fail_state_delete<B: crate::backend::Backend>(svc: &TeleS3<B>) {
        svc.repo
            .db
            .execute_unprepared(
                "CREATE TRIGGER fail_upload_delete BEFORE DELETE ON s3_multipart_upload_state \
                 BEGIN SELECT RAISE(ABORT, 'injected upload delete failure'); END",
            )
            .await
            .expect("create upload delete trigger");
    }

    async fn allow_state_delete<B: crate::backend::Backend>(svc: &TeleS3<B>) {
        svc.repo
            .db
            .execute_unprepared("DROP TRIGGER fail_upload_delete")
            .await
            .expect("drop upload delete trigger");
    }

    /// Make `get_object` fail while everything the completion path touches
    /// before it -- the upload state and its parts -- keeps working.
    ///
    /// `get_object` reads `s3_object`, so renaming that table away turns the
    /// lookup into a hard SQL error. `s3_multipart_upload_state` and
    /// `s3_blob` are untouched, so the request really does reach the
    /// replaced-object lookup: the *query itself* fails there, which is
    /// exactly the condition a handler that folds every error into "nothing
    /// to release" cannot distinguish from an absent key.
    ///
    /// The table is renamed rather than dropped because `s3_object` holds a
    /// foreign key into it and SQLite refuses to drop a referenced parent.
    async fn break_object_lookup<B: crate::backend::Backend>(svc: &TeleS3<B>) {
        svc.repo
            .db
            .execute_unprepared("ALTER TABLE s3_object RENAME TO s3_object_hidden")
            .await
            .expect("rename s3_object away");
    }

    /// A failed read of the object being replaced must abort the completion,
    /// not report success while silently skipping the blob release.
    ///
    /// Before the fix this site read `if let Ok(model) = ...get_object(...)`,
    /// so a repository failure landed in the `else` arm and the previous
    /// object's blob references were never released.
    ///
    /// This is the end-to-end contract check. It cannot isolate the lookup on
    /// its own: `cas_put_object` writes the same `s3_object` table later in
    /// the request, so a fault injected at table level surfaces there too
    /// either way. `replaced_object_propagates_a_repository_read_failure` in
    /// `s3::objects` pins the swallow itself against the shared helper both
    /// write paths evaluate.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn complete_multipart_propagates_object_read_failure() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");
        break_object_lookup(&svc).await;

        let err = svc
            .complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect_err("a failed replaced-object read must not be swallowed");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    /// The guard against over-correcting: a key that holds no object is the
    /// normal outcome of a first completion, and must stay a silent no-op.
    ///
    /// `get_object` reports that absence as `Err(NoSuchKey)`, so propagating
    /// every error -- rather than only `NoSuchKey` -- would break completing
    /// an upload onto a fresh key.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn complete_multipart_onto_absent_key_is_not_an_error() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");

        let res = svc
            .complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect("completing onto an absent key is a normal first write");
        assert_eq!(res.output.key.as_deref(), Some("k"));
    }

    /// A failed state delete must roll the object write back, so the upload
    /// still owns its part references and a retry publishes usable bytes.
    ///
    /// Before the fix the object row was committed first and the state delete
    /// ran in a second transaction. A retry then released the previous
    /// object's references after deleting the state, which left the new
    /// object pointing at a blob that no reference protected.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn complete_multipart_keeps_the_upload_when_the_state_delete_fails() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("read upload state");
        let blob_id = upload_blob_ids(&state.content)
            .into_iter()
            .next()
            .expect("part blob id");

        fail_state_delete(&svc).await;
        let err = svc
            .complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect_err("a failed state delete must fail the completion");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        assert!(
            svc.repo.get_object("b", "k").await.is_err(),
            "a failed completion must not publish the object"
        );
        svc.repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("the upload must survive for a retry");
        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            1,
            "the surviving upload must still own the part reference"
        );

        allow_state_delete(&svc).await;
        svc.complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect("retry completion");

        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            1,
            "the completed object must own the part reference"
        );
        assert!(
            matches!(
                svc.backend
                    .read(blob_id, 0, None)
                    .await
                    .expect("read completed blob"),
                ReadOutcome::Present(_)
            ),
            "the completed bytes must survive the retry"
        );
    }

    /// A repeated upload of one part number must release the blob that the
    /// replaced part was the last holder of.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn uploading_a_part_again_releases_the_replaced_blob() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("first upload part");

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("read upload state");
        let first_blob = upload_blob_ids(&state.content)
            .into_iter()
            .next()
            .expect("first blob id");

        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("second upload part");

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("read upload state");
        let second_blob = upload_blob_ids(&state.content)
            .into_iter()
            .next()
            .expect("second blob id");
        assert_ne!(first_blob, second_blob, "the retry must store new bytes");
        assert_eq!(
            svc.repo
                .blob_ref_count(&first_blob)
                .await
                .expect("first ref count"),
            0,
            "the replaced blob must lose its reference"
        );
        assert!(
            matches!(
                svc.backend
                    .read(first_blob, 0, None)
                    .await
                    .expect("read replaced blob"),
                ReadOutcome::Missing
            ),
            "the replaced blob must be removed from the backend"
        );
        assert_eq!(
            svc.repo
                .blob_ref_count(&second_blob)
                .await
                .expect("second ref count"),
            1,
            "the stored part must own the new blob"
        );
    }

    /// A failed release of the replaced object's references must roll the
    /// completion back, so the old object keeps the bytes it still owns.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn complete_multipart_keeps_the_old_object_when_the_release_fails() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("read upload state");
        let blob_id = upload_blob_ids(&state.content)
            .into_iter()
            .next()
            .expect("part blob id");

        // The old object shares the part blob, so completing it must drop
        // only the old object's reference and keep the bytes.
        svc.repo
            .acquire_blob_refs(&[(blob_id.clone(), 1)])
            .await
            .expect("acquire old object reference");
        svc.repo
            .upsert_object(
                "b".into(),
                "k".into(),
                ObjectWrite {
                    size: 1,
                    content_type: None,
                    etag: Some("old-etag".into()),
                    content: serde_json::json!({
                        "item": [{"id": blob_id, "offset": 0, "size": 1}]
                    }),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    tags: serde_json::json!([]),
                },
            )
            .await
            .expect("seed old object");

        fail_blob_updates(&svc).await;
        svc.complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect_err("a failed release must fail the completion");
        svc.repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("the upload must survive for a retry");
        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            2,
            "the old object and the upload must keep their references"
        );
        assert_eq!(
            svc.repo
                .get_object("b", "k")
                .await
                .expect("old object")
                .etag,
            Some("old-etag".into()),
            "the old object row must stay in place"
        );
        assert!(
            matches!(
                svc.backend
                    .read(blob_id.clone(), 0, None)
                    .await
                    .expect("read shared blob"),
                ReadOutcome::Present(_)
            ),
            "the old object's bytes must stay readable"
        );

        allow_blob_updates(&svc).await;
        svc.complete_multipart_upload_inner(complete_upload_req(&upload_id))
            .await
            .expect("retry completion");

        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            1,
            "the completed object must own the only remaining reference"
        );
        assert!(
            matches!(
                svc.backend
                    .read(blob_id, 0, None)
                    .await
                    .expect("read shared blob"),
                ReadOutcome::Present(_)
            ),
            "the shared bytes must survive the release of the old object"
        );
    }

    /// A failed blob release must leave the upload in place, not delete the
    /// only record of references that are still alive.
    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn abort_multipart_keeps_the_upload_when_the_blob_release_fails() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("read upload state");
        let blob_id = upload_blob_ids(&state.content)
            .into_iter()
            .next()
            .expect("part blob id");

        fail_blob_updates(&svc).await;
        let err = svc
            .abort_multipart_upload_inner(abort_upload_req(&upload_id))
            .await
            .expect_err("a failed release must fail the abort");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        svc.repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("the upload must survive for a retry");
        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            1,
            "a rolled-back abort must not drop the part reference"
        );

        allow_blob_updates(&svc).await;
        svc.abort_multipart_upload_inner(abort_upload_req(&upload_id))
            .await
            .expect("retry abort");

        assert_eq!(
            svc.repo.blob_ref_count(&blob_id).await.expect("ref count"),
            0,
            "a successful abort must release the part reference"
        );
        assert!(
            matches!(
                svc.backend
                    .read(blob_id, 0, None)
                    .await
                    .expect("read aborted blob"),
                ReadOutcome::Missing
            ),
            "a successful abort must remove unreferenced bytes"
        );
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn negative_max_uploads_is_invalid_argument() {
        let (svc, _upload_id) = test_service_with_upload().await;

        let err = svc
            .list_multipart_uploads_inner(list_uploads_req(Some(-1), None, None))
            .await
            .expect_err("negative max-uploads must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn list_uploads_paginates_and_reports_next_markers() {
        let (svc, _upload_id) = test_service_with_upload().await;
        svc.repo
            .upsert_multipart_upload_state(MultipartUploadStateUpdate {
                bucket: "b".into(),
                key: "k".into(),
                upload_id: "u2".into(),
                content_type: None,
                user_metadata: serde_json::json!({}),
                tags: serde_json::json!([]),
                content: serde_json::json!({}),
            })
            .await
            .expect("create second upload state");

        let page1 = svc
            .list_multipart_uploads_inner(list_uploads_req(Some(1), None, None))
            .await
            .expect("page 1")
            .output;
        let uploads = page1.uploads.expect("uploads");
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].key.as_deref(), Some("k"));
        assert_eq!(uploads[0].upload_id.as_deref(), Some("u1"));
        assert_eq!(page1.is_truncated, Some(true));
        assert_eq!(page1.next_key_marker.as_deref(), Some("k"));
        assert_eq!(page1.next_upload_id_marker.as_deref(), Some("u1"));

        let page2 = svc
            .list_multipart_uploads_inner(list_uploads_req(
                Some(1),
                page1.next_key_marker,
                page1.next_upload_id_marker,
            ))
            .await
            .expect("page 2")
            .output;
        let uploads = page2.uploads.expect("uploads");
        assert_eq!(uploads.len(), 1);
        assert_eq!(uploads[0].upload_id.as_deref(), Some("u2"));
        assert_eq!(page2.is_truncated, Some(false));
        assert_eq!(page2.next_key_marker, None);
        assert_eq!(page2.next_upload_id_marker, None);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn negative_max_parts_is_invalid_argument() {
        let (svc, upload_id) = test_service_with_upload().await;

        let err = svc
            .list_parts_inner(list_parts_req(&upload_id, Some(-1)))
            .await
            .expect_err("negative max-parts must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn negative_content_length_is_invalid_argument() {
        let (svc, upload_id) = test_service_with_upload().await;

        let err = svc
            .upload_part_inner(upload_part_req(&upload_id, Some(-1)))
            .await
            .expect_err("negative content-length must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = svc
            .upload_part_inner(upload_part_req(&upload_id, None))
            .await
            .expect_err("missing content-length must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::MissingContentLength);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn upload_part_reports_the_specific_checksum_failure() {
        let (svc, upload_id) = test_service_with_upload().await;

        // A decodable but wrong checksum is a digest mismatch.
        let err = svc
            .upload_part_inner(upload_part_with_crc32_req(&upload_id, "AAAAAA=="))
            .await
            .expect_err("a mismatching crc32 must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);

        // An undecodable checksum is a different condition and must not be
        // flattened into the same BadDigest.
        let err = svc
            .upload_part_inner(upload_part_with_crc32_req(&upload_id, "not base64!"))
            .await
            .expect_err("an undecodable crc32 must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidDigest);
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn a_successful_part_resolves_its_publish_intent() {
        let (svc, upload_id) = test_service_with_upload().await;
        svc.upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect("upload part");

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "exactly one intent per part: {rows:?}");
        assert!(
            rows[0].resolved_at.is_some(),
            "a confirmed part must resolve its intent: {rows:?}"
        );
        assert!(
            rows[0].blob_id.is_some(),
            "a resolved intent must record the blob id: {rows:?}"
        );
    }

    #[tokio::test]
    #[allow(clippy::expect_used)]
    async fn a_failed_intent_resolve_keeps_the_part_out_of_the_upload() {
        // The content update and the resolve commit together. Otherwise the
        // upload would link a part whose intent stays pending, and the reclaim
        // pass would later delete the bytes of that live part.
        let (svc, upload_id) = test_service_with_upload().await;
        refuse_publish_intent_updates(&svc).await;

        let err = svc
            .upload_part_inner(upload_one_byte_part_req(&upload_id))
            .await
            .expect_err("a failed resolve must fail the part upload");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let state = svc
            .repo
            .get_multipart_upload_state("b", "k", &upload_id)
            .await
            .expect("load upload state");
        assert_eq!(
            state.content,
            serde_json::json!({}),
            "the failed resolve must roll back the part"
        );
        assert!(
            blob_rows(&svc).await.is_empty(),
            "the failed part must not leave a blob row behind"
        );
        assert!(
            intents(&svc).await.is_empty(),
            "the released part settles its publish, so no intent must remain"
        );
    }
}
