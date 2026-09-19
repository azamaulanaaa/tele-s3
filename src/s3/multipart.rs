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
    StreamingBlobExt, build_put_condition, chrono_to_timestamp, metadata_to_json,
};
use super::repo::ObjectWrite;
use super::types::{Metadata, MetadataItem, MultipartUploadPart};
use crate::backend::{Backend, ChainReaders, ReaderWithHasher};

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
    pub(crate) async fn upload_part_inner(
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
                        serde_json::from_value(content.take()).map_err(S3Error::internal_error)?;

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
                    && let Ok(bytes) = hex::decode(&part.hash)
                {
                    hashes_byte.extend(bytes);
                }
            }

            let hash_md5 = md5::Md5::digest(&hashes_byte);
            let hash_md5 = hex::encode(hash_md5);

            Some(format!("{}-{}", hash_md5, part_count))
        };

        let versioning_status = self.repo.get_bucket_versioning(&req.input.bucket).await?;
        let is_versioned = versioning_status.is_some();

        let delete_old_object_future = if is_versioned {
            None
        } else {
            let model = self
                .repo
                .get_object(&req.input.bucket, &req.input.key)
                .await;
            if let Ok(model) = model {
                let metadata: Metadata =
                    serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
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
            checksums: serde_json::json!({}),
        };

        let version_id = self
            .repo
            .cas_put_object(
                req.input.bucket.clone(),
                req.input.key.clone(),
                data,
                condition,
            )
            .await?;

        self.repo
            .delete_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        if let Some(delete_old_object_future) = delete_old_object_future {
            delete_old_object_future.await?;
        }

        let dangling_ids: Vec<String> = content
            .into_values()
            .flat_map(|multipart_upload_part| {
                multipart_upload_part
                    .metadata_items
                    .into_iter()
                    .map(|i| i.id)
            })
            .collect();
        self.release_blobs(dangling_ids).await?;

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
                multipart_upload_part
                    .metadata_items
                    .into_iter()
                    .map(|i| i.id)
            })
            .collect();
        self.release_blobs(part_ids).await?;

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
    pub(crate) async fn list_multipart_uploads_inner(
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
    pub(crate) async fn upload_part_copy_inner(
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
        let acquire_items: Vec<(String, u64)> =
            part_items.iter().map(|v| (v.id.clone(), v.size)).collect();
        self.repo.acquire_blob_refs(&acquire_items).await?;

        // Compute the slice digest by streaming the shared bytes back once.
        // This costs a read pass through the backend per copied part but
        // keeps real ETags on every part and preserves the combined
        // multipart ETag algorithm. Each slice is already a (blob, offset,
        // length) triple, so one ranged backend read per slice suffices.
        let part_hash = {
            let reader_futures = part_items.iter().map(|item| {
                self.backend
                    .read(item.id.clone(), item.offset, Some(item.size))
            });

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
                        serde_json::from_value(content.take()).map_err(S3Error::internal_error)?;

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
}
