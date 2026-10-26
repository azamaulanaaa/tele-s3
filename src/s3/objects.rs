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
    ExpectedChecksums, StreamingBlobExt, build_put_condition, canned_owner, check_conditional_get,
    chrono_to_timestamp, delete_marker_error, full_control_grant, json_to_metadata,
    json_to_tag_set, metadata_to_json, tagging_header_to_json, tags_to_json, verify_checksums,
};
use super::repo::entity::object;
use super::repo::{ObjectWrite, Repository};
use super::types::{Metadata, MetadataItem};
use crate::backend::{
    Backend, BoxedAsyncReader, ChainReaders, IntegrityDigests, IntegrityReader, ReadOutcome,
    new_publish_id,
};

/// Map one [`ReadOutcome`] to the reader this read path needs.
///
/// The metadata store resolved these blobs *before* the backend was asked
/// for them, so both non-present variants are failures here: `Missing` means
/// bytes that were promised are gone, and `Fault` means the backend cannot
/// serve bytes it holds. Neither is a client error — the object was resolved,
/// so it exists and a `NoSuchKey` would be a lie. Both answer
/// [`S3ErrorCode::InternalError`] with that code's own generic text; what
/// distinguished them reaches the server log, as the fault's error source.
pub(super) fn read_outcome_to_reader(outcome: ReadOutcome) -> S3Result<BoxedAsyncReader> {
    match outcome {
        ReadOutcome::Present(reader) => Ok(reader),
        ReadOutcome::Missing => Err(S3Error::new(S3ErrorCode::InternalError)),
        ReadOutcome::Fault(err) => Err(S3Error::from(err)),
    }
}

/// Upper bound the S3 API applies to `max-keys`; larger client values are
/// silently clamped rather than rejected.
pub(super) const MAX_KEYS_CEILING: i32 = 1000;

/// Validate a client-supplied paging size and apply the S3 ceiling.
///
/// `s3s` parses `max-keys` as a plain `i32` with no range check, so a
/// negative value must be rejected here instead of being cast into a huge
/// unsigned limit further down.
pub(super) fn checked_max_keys(max_keys: Option<i32>) -> S3Result<i32> {
    let raw = max_keys.unwrap_or(MAX_KEYS_CEILING);
    if raw < 0 {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument));
    }
    Ok(raw.min(MAX_KEYS_CEILING))
}

/// Validate the `max-uploads` paging size of ListMultipartUploads.
///
/// `max-uploads` is bounded by the same S3 ceiling as `max-keys` and has the
/// same unset default, so the rule lives in one place; this alias keeps the
/// call site in the multipart listing readable.
pub(super) fn checked_max_uploads(max_uploads: Option<i32>) -> S3Result<i32> {
    checked_max_keys(max_uploads)
}

pub(super) fn checked_content_length(content_length: Option<i64>) -> S3Result<u64> {
    let len = content_length.ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))?;
    u64::try_from(len).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))
}

/// The listing-row fields the rollup reads: key, size and last-modified.
///
/// `ObjectListRow` lives in the repository module, so callers map their rows
/// into this tuple rather than naming the DTO.
type ListRow<'a> = (&'a str, u32, chrono::DateTime<chrono::Utc>);

/// The `CommonPrefix` a key rolls into, or `None` when it is listed whole.
///
/// The part of `key` after `prefix` is split at its first `delimiter`; the
/// common prefix is `prefix` plus the segment before it. A key with no
/// delimiter after the prefix is its own `Object`.
fn common_prefix_for(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    let without_prefix = key.strip_prefix(prefix).unwrap_or(key);

    without_prefix
        .split_once(delimiter)
        .map(|(sub_key, _)| format!("{prefix}{sub_key}{delimiter}"))
}

/// Roll one page of listing rows into `Object`s and `CommonPrefix`es, and
/// derive the cursor that resumes the page after it.
///
/// Both list shapes (v1 and v2) roll up identically, so the rule lives here
/// once. `truncated` is the raw-row overflow flag; the cursor is the last
/// row's key, and is present only when that flag is set.
fn rollup_list_page<'a>(
    rows: impl IntoIterator<Item = ListRow<'a>>,
    prefix: &str,
    delimiter: Option<&str>,
    truncated: bool,
) -> (Vec<Object>, Vec<CommonPrefix>, Option<String>) {
    let mut last_key = None;

    let (contents, common_prefixes) = rows.into_iter().fold(
        (Vec::<Object>::new(), Vec::<CommonPrefix>::new()),
        |mut result, (key, size, last_modified)| {
            last_key = Some(key.to_owned());

            match delimiter.and_then(|delimiter| common_prefix_for(key, prefix, delimiter)) {
                Some(common_prefix) => result.1.push(CommonPrefix {
                    prefix: Some(common_prefix),
                }),
                None => result.0.push(Object {
                    key: Some(key.to_owned()),
                    size: Some(size.into()),
                    last_modified: Some(chrono_to_timestamp(last_modified)),
                    ..Default::default()
                }),
            }

            result
        },
    );

    let next_marker = truncated.then_some(last_key).flatten();

    (contents, common_prefixes, next_marker)
}

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(self), err)]
    pub(crate) async fn put_object_inner(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let size = checked_content_length(req.input.content_length)?;

        // The blob row's `size` column is 32-bit; reject an oversized body up
        // front so the failure still precedes the backend write, exactly as
        // it did when the row was inserted separately.
        Repository::checked_size(size)?;

        let condition = build_put_condition(
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
        )?;

        // Fast-fail doomed conditional writes before streaming into the
        // backend; the atomic re-check still happens at publish commit time.
        self.precondition_gate(&req.input.bucket, &req.input.key, &condition)
            .await?;

        let expected = ExpectedChecksums::new(
            req.input.checksum_crc32.clone(),
            req.input.checksum_crc32c.clone(),
            req.input.checksum_sha1.clone(),
            req.input.checksum_sha256.clone(),
        );

        let checksums = expected.to_json()?;

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        // The publish identifier is captured in an outer option so that the
        // intent can be resolved (or cleared) at the right point later on:
        // resolution must wait until the metadata publish commits, and
        // clearing happens on the checksum/content_md5/discard paths. A
        // `None` here means a zero-size object, which never recorded an
        // intent.
        let mut publish_id: Option<String> = None;

        let (id, hash_md5, computed_crc32, computed_crc32c, computed_sha1, computed_sha256) = {
            let state = Arc::new(Mutex::new(IntegrityDigests::default()));

            let id = if size > 0 {
                let reader_with_hasher = Box::pin(IntegrityReader::new(reader, state.clone()));

                // The publish intent is recorded *before* the backend is
                // called, because the failure this exists for is a call whose
                // answer never arrives. If the row cannot be written the
                // backend must not be called at all: a publish with no record
                // of it is unrecoverable, so failing here is strictly safer.
                let pid = new_publish_id();
                self.repo
                    .record_publish_intent(
                        pid.clone(),
                        req.input.bucket.clone(),
                        req.input.key.clone(),
                        size,
                    )
                    .await?;

                publish_id = Some(pid.clone());

                let id = self
                    .backend
                    .write(pid.clone(), size, reader_with_hasher)
                    .await
                    .map_err(S3Error::from)?;

                // Do NOT resolve the intent here: a later failure (checksum
                // mismatch, content_md5 mismatch, or a failed metadata
                // publish) would otherwise leave a resolved row that the
                // reconciler cannot reclaim. Resolution is deferred until the
                // metadata publish commits (see below).

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

        // The backend blob now exists but nothing references it yet: its row
        // is written by the publish transaction further down, together with
        // the object row. Verify streaming checksums before that link is
        // made. A mismatch means corrupt transport: drop the backend blob and
        // fail fast so archives never store bad bytes under a good checksum.
        if let Err(e) = verify_checksums(
            computed_crc32,
            computed_crc32c,
            &computed_sha1,
            &computed_sha256,
            &expected,
        ) {
            let discarded = match id {
                Some(id) => self.discard_backend_blobs(vec![id]).await,
                None => true,
            };
            // Clear the intent only after a confirmed delete. If the delete
            // failed, the pending row is the only record of the bytes that
            // are still in the backend.
            if discarded && let Some(pid) = &publish_id {
                let _ = self
                    .repo
                    .clear_publish_intents(std::slice::from_ref(pid))
                    .await;
            }
            return Err(e);
        }

        if let Some(expected) = req.input.content_md5 {
            // Decode instead of comparing strings so padding/whitespace
            // variations in the client header don't cause false mismatches.
            let expected_digest = match base64::prelude::BASE64_STANDARD.decode(expected.trim()) {
                Ok(v) => v,
                Err(_) => {
                    let discarded = match id {
                        Some(id) => self.discard_backend_blobs(vec![id]).await,
                        None => true,
                    };
                    if discarded && let Some(pid) = &publish_id {
                        let _ = self
                            .repo
                            .clear_publish_intents(std::slice::from_ref(pid))
                            .await;
                    }
                    return Err(S3Error::new(S3ErrorCode::InvalidDigest));
                }
            };

            if expected_digest[..] != hash_md5[..] {
                let discarded = match id {
                    Some(id) => self.discard_backend_blobs(vec![id]).await,
                    None => true,
                };

                // Clear the intent only after a confirmed delete. If the
                // delete failed, the pending row is the only record of the
                // bytes that are still in the backend.
                if discarded && let Some(pid) = &publish_id {
                    let _ = self
                        .repo
                        .clear_publish_intents(std::slice::from_ref(pid))
                        .await;
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
            match self
                .replaced_object(&req.input.bucket, &req.input.key)
                .await?
            {
                Some(model) => {
                    let metadata: Metadata =
                        serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
                    Some(self.release_blobs(metadata.item.into_iter().map(|v| v.id).collect()))
                }
                None => None,
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

            // The blob row, the object row and the resolved intent are
            // published in ONE transaction: a failure rolls all of them back,
            // so no orphaned `s3_blob` row and no pending intent for a live
            // object can survive the write. The caller then drops the backend
            // bytes the rolled-back row would have owned.
            let result = match (publish_id.as_deref(), id.clone()) {
                (Some(pid), Some(blob_id)) => {
                    self.repo
                        .publish_object_with_new_blob(
                            blob_id,
                            Some(pid),
                            bucket,
                            key,
                            data,
                            condition,
                        )
                        .await
                }
                (None, None) => self.repo.cas_put_object(bucket, key, data, condition).await,
                // The identifier and the blob are set together by the
                // `size > 0` branch above.
                _ => Err(S3Error::internal_error(std::io::Error::other(
                    "a write with a blob must carry a publish intent",
                ))),
            };

            match result {
                Ok(vid) => vid,
                Err(err) => {
                    if let Some(id) = id {
                        let _ = self.discard_backend_blobs(vec![id]).await;
                    }
                    // Leave the intent pending: the reconciler can reclaim
                    // the (possibly published) backend blob. Do NOT clear it.
                    return Err(err);
                }
            }
        };

        if let Some(delete_old_future) = delete_old_future {
            delete_old_future.await?;
        }

        let stored_checksums = ExpectedChecksums::from_json(&checksums);

        let response_version_id = if is_versioned { Some(version_id) } else { None };

        let res = S3Response::new(PutObjectOutput {
            e_tag: etag.map(ETag::Strong),
            size: Some(size as i64),
            checksum_crc32: stored_checksums.crc32,
            checksum_crc32c: stored_checksums.crc32c,
            checksum_sha1: stored_checksums.sha1,
            checksum_sha256: stored_checksums.sha256,
            version_id: response_version_id,
            ..Default::default()
        });

        Ok(res)
    }

    /// The latest object a copy is about to replace, or `None` when the
    /// destination key holds nothing to release.
    ///
    /// `get_object` reports absence as `Err(NoSuchKey)` rather than
    /// `Ok(None)`, so "the key is empty" and "the read failed" arrive on the
    /// same error channel. Folding both into the old `if let Ok(..)` skipped
    /// the blob release on every repository failure, leaking the previous
    /// object's blob references; propagating both, on the other hand, turns
    /// every copy onto a fresh key into a client-visible error.
    ///
    /// Only `NoSuchKey` is folded into `None`. Every other error propagates,
    /// aborting the request. Do NOT simplify this into a blanket `?`.
    pub(crate) async fn replaced_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<object::Model>> {
        match self.repo.get_object(bucket, key).await {
            Ok(model) => Ok(Some(model)),
            Err(err) if *err.code() == S3ErrorCode::NoSuchKey => Ok(None),
            Err(err) => Err(err),
        }
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn copy_object_inner(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
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

        // Absence is a normal outcome here (copying onto a key that does not
        // exist yet) and must stay a silent no-op; a failed read must not.
        let old_object = self
            .replaced_object(&req.input.bucket, &req.input.key)
            .await?;

        let delete_old_future = if is_versioned {
            None
        } else if let Some(old) = old_object {
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

    /// Resolve the object a read (`GetObject` / `HeadObject`) addresses.
    ///
    /// Addressing a delete marker by version is MethodNotAllowed (405); a
    /// latest-key read of one reads as NoSuchKey, but S3 still reports which
    /// marker hides the key. Both operations answer from the same row, so the
    /// rule lives here once.
    async fn resolve_read_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<object::Model> {
        if let Some(vid) = version_id {
            let model = self.repo.get_object_version(bucket, key, vid).await?;
            if model.is_delete_marker {
                return Err(delete_marker_error(
                    &model.version_id,
                    S3ErrorCode::MethodNotAllowed,
                ));
            }
            return Ok(model);
        }

        // One latest-row read covers both the object and the delete-marker case.
        match self.repo.get_latest_raw(bucket, key).await? {
            Some(model) if !model.is_delete_marker => Ok(model),
            Some(marker) if marker.is_delete_marker => Err(delete_marker_error(
                &marker.version_id,
                S3ErrorCode::NoSuchKey,
            )),
            _ => Err(S3Error::new(S3ErrorCode::NoSuchKey)),
        }
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_object_inner(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = self
            .resolve_read_object(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
            )
            .await?;

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
            .map(read_outcome_to_reader)
            .collect::<S3Result<Vec<_>>>()?;

        let chain_readers = ChainReaders::from_vec(readers);

        let body = StreamingBlob::wrap(chain_readers);

        let object_metadata = json_to_metadata(&model.user_metadata);
        let stored_checksums = ExpectedChecksums::from_json(&model.checksums);

        let versioning = self.repo.get_bucket_versioning(&req.input.bucket).await?;
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
            checksum_crc32: stored_checksums.crc32,
            checksum_crc32c: stored_checksums.crc32c,
            checksum_sha1: stored_checksums.sha1,
            checksum_sha256: stored_checksums.sha256,
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
        let model = self
            .resolve_read_object(
                &req.input.bucket,
                &req.input.key,
                req.input.version_id.as_deref(),
            )
            .await?;

        check_conditional_get(
            &model,
            req.input.if_match.as_ref(),
            req.input.if_none_match.as_ref(),
            req.input.if_modified_since.as_ref(),
            req.input.if_unmodified_since.as_ref(),
        )?;

        let stored_checksums = ExpectedChecksums::from_json(&model.checksums);

        let versioning = self.repo.get_bucket_versioning(&req.input.bucket).await?;
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
            checksum_crc32: stored_checksums.crc32,
            checksum_crc32c: stored_checksums.crc32c,
            checksum_sha1: stored_checksums.sha1,
            checksum_sha256: stored_checksums.sha256,
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
            let metadata: Metadata =
                serde_json::from_value(model.content.clone()).map_err(S3Error::internal_error)?;
            let ids: Vec<String> = metadata.item.iter().map(|item| item.id.clone()).collect();
            self.release_blobs(ids).await?;
        }
        // If we created a delete marker, `deleted_opt` is the marker; no blobs to release.

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
        // A missing bucket is a real S3 answer, not a database fault: it is
        // reported per key below alongside every other per-key failure. Only a
        // genuine repository error aborts the whole request -- letting one
        // through as "unversioned" would take the destructive permanent-delete
        // path and discard the previous versions.
        let bucket_versioning = match self.repo.get_bucket_versioning(&req.input.bucket).await {
            Ok(versioning) => versioning,
            Err(err) if *err.code() == S3ErrorCode::NoSuchBucket => None,
            Err(err) => return Err(err),
        };
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
                        deleted_objects.push(DeletedObject {
                            key: Some(obj.key.clone()),
                            version_id: obj.version_id.clone(),
                            ..Default::default()
                        });
                    }
                    Err(e) if *e.code() == S3ErrorCode::NoSuchKey => {
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
        let max_keys = checked_max_keys(req.input.max_keys)?;
        let limit =
            u64::try_from(max_keys).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;

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

        let (contents, common_prefixes, next_marker) = rollup_list_page(
            models
                .iter()
                .map(|model| (model.id.as_str(), model.size, model.last_modified)),
            req.input.prefix.as_deref().unwrap_or_default(),
            req.input.delimiter.as_deref(),
            is_truncated,
        );

        let is_truncated = next_marker.is_some();

        let res = S3Response::new(ListObjectsOutput {
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            is_truncated: Some(is_truncated),
            marker: req.input.marker,
            next_marker,
            max_keys: Some(max_keys),
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
        let max_keys = checked_max_keys(req.input.max_keys)?;
        let limit =
            u64::try_from(max_keys).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;

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

        let (contents, common_prefixes, next_marker) = rollup_list_page(
            models
                .iter()
                .map(|model| (model.id.as_str(), model.size, model.last_modified)),
            req.input.prefix.as_deref().unwrap_or_default(),
            req.input.delimiter.as_deref(),
            is_truncated,
        );

        let is_truncated = next_marker.is_some();

        let res = S3Response::new(ListObjectsV2Output {
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            is_truncated: Some(is_truncated),
            next_continuation_token: next_marker,
            key_count: Some(models.len() as i32),
            max_keys: Some(max_keys),
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
        let max_keys = checked_max_keys(req.input.max_keys)?;
        let limit =
            usize::try_from(max_keys).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
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
        let mut combined = versions
            .into_iter()
            .chain(delete_markers)
            .collect::<Vec<_>>();
        combined.sort_by(|a, b| {
            a.id.cmp(&b.id)
                .then_with(|| b.last_modified.cmp(&a.last_modified))
        });

        let is_truncated = limit > 0 && combined.len() > limit;
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

        // The repository returns projected listing rows, not full
        // `object::Model`s: this handler reads only `id`, `version_id`,
        // `is_latest`, `is_delete_marker`, `size`, `last_modified` and `etag`,
        // so the JSON columns are never fetched.
        let mut versions: Vec<_> = Vec::new();
        let mut delete_markers: Vec<_> = Vec::new();
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
        let mut filtered_versions: Vec<_> = Vec::new();
        let mut filtered_delete_markers: Vec<_> = Vec::new();

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

    /// Confirm the addressed object exists before answering an ACL request.
    ///
    /// Both ACL verbs answer from the same canned grant and differ only in
    /// whether they are addressed to a specific version.
    async fn ensure_object_exists(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<()> {
        match version_id {
            Some(vid) => self.repo.get_object_version(bucket, key, vid).await?,
            None => self.repo.get_object(bucket, key).await?,
        };
        Ok(())
    }

    /// The version a tagging write reports back.
    ///
    /// Without an explicit `versionId` the write applies to the latest
    /// version, which is only reported on a versioned bucket.
    async fn tagging_write_version_id(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<Option<String>> {
        if let Some(vid) = version_id {
            return Ok(Some(vid.to_owned()));
        }

        let versioned = self.repo.get_bucket_versioning(bucket).await?.is_some();

        if versioned {
            self.repo.latest_version_id(bucket, key).await
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self), err)]
    pub(crate) async fn get_object_acl_inner(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        self.ensure_object_exists(
            &req.input.bucket,
            &req.input.key,
            req.input.version_id.as_deref(),
        )
        .await?;
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
        self.ensure_object_exists(
            &req.input.bucket,
            &req.input.key,
            req.input.version_id.as_deref(),
        )
        .await?;
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
            let vs = self.repo.get_bucket_versioning(&req.input.bucket).await?;
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
        let version_id = self
            .tagging_write_version_id(&req.input.bucket, &req.input.key, vid.as_deref())
            .await?;
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
        let version_id = self
            .tagging_write_version_id(&req.input.bucket, &req.input.key, vid.as_deref())
            .await?;
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
    use crate::backend::{BackendError, Memory};
    use crate::s3::TeleS3;
    use sea_orm::{ConnectionTrait, EntityTrait};

    async fn test_service() -> TeleS3<Memory<1, 1>> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(Memory::default(), db).await.expect("init")
    }

    /// A backend whose writes can be made to fail on demand, so what the
    /// publish intent survives is observable.
    struct FlakyWrite {
        inner: Memory<8, 8>,
        fail_write: std::sync::atomic::AtomicBool,
        fail_delete: std::sync::atomic::AtomicBool,
    }

    impl FlakyWrite {
        fn fail_writes(&self, fail: bool) {
            self.fail_write
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }

        fn fail_deletes(&self, fail: bool) {
            self.fail_delete
                .store(fail, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl Backend for FlakyWrite {
        async fn write(
            &self,
            publish_id: String,
            size: u64,
            reader: BoxedAsyncReader,
        ) -> Result<String, BackendError> {
            if self.fail_write.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BackendError::Other("write refused".into()));
            }

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
            if self.fail_delete.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(BackendError::Other("delete refused".into()));
            }
            self.inner.delete(key).await
        }

        async fn reclaim_publish(
            &self,
            publish_id: String,
        ) -> Result<crate::backend::ReclaimOutcome, BackendError> {
            self.inner.reclaim_publish(publish_id).await
        }
    }

    async fn flaky_write_service() -> TeleS3<FlakyWrite> {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        TeleS3::init(
            FlakyWrite {
                inner: Memory::default(),
                fail_write: std::sync::atomic::AtomicBool::new(false),
                fail_delete: std::sync::atomic::AtomicBool::new(false),
            },
            db,
        )
        .await
        .expect("init")
    }

    async fn intents(
        svc: &TeleS3<impl Backend>,
    ) -> Vec<crate::s3::repo::entity::publish_intent::Model> {
        crate::s3::repo::entity::publish_intent::Entity::find()
            .all(&svc.repo.db)
            .await
            .expect("load publish intents")
    }

    #[test]
    fn a_present_read_outcome_yields_the_reader() {
        let reader: BoxedAsyncReader = Box::pin(futures::io::Cursor::new(b"bytes".to_vec()));

        assert!(read_outcome_to_reader(ReadOutcome::Present(reader)).is_ok());
    }

    #[test]
    fn a_missing_read_outcome_is_an_internal_error_and_never_no_such_key() {
        // The metadata row promised these bytes before the backend was
        // called, so a genuine absence here is lost data, not a missing
        // object. This is the behaviour the old `Option` had and must keep.
        let err = match read_outcome_to_reader(ReadOutcome::Missing) {
            Ok(_) => panic!("a missing blob must not yield a reader"),
            Err(err) => err,
        };

        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    #[test]
    fn a_fault_read_outcome_is_an_internal_error_that_hides_the_fault() {
        let err =
            match read_outcome_to_reader(ReadOutcome::Fault(BackendError::InvariantViolation {
                detail: "key is not a message id this backend minted",
            })) {
                Ok(_) => panic!("a faulted blob must not yield a reader"),
                Err(err) => err,
            };

        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        // The detail is what tells an operator reading the *log* whether the
        // object was deleted or the storage is corrupt, so it must survive as
        // the error's source — and must not be the body the client reads.
        let detail = err
            .source()
            .expect("the backend fault is kept as the error source")
            .to_string();
        assert!(detail.contains("key is not a message id this backend minted"));

        let message = err.message().unwrap_or_default();
        assert!(
            !message.contains("message id") && !message.contains("invariant"),
            "the internal detail must not reach the client: {message}"
        );
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

    fn get_tagging_req(
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Request<GetObjectTaggingInput> {
        S3Request {
            input: GetObjectTaggingInput {
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

    fn put_tagging_req(
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Request<PutObjectTaggingInput> {
        S3Request {
            input: PutObjectTaggingInput {
                bucket: bucket.to_string(),
                checksum_algorithm: None,
                content_md5: None,
                expected_bucket_owner: None,
                key: key.to_string(),
                request_payer: None,
                tagging: s3s::dto::Tagging {
                    tag_set: vec![s3s::dto::Tag {
                        key: Some("t".to_string()),
                        value: Some("1".to_string()),
                    }],
                },
                version_id,
            },
            method: http::Method::PUT,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn delete_tagging_req(
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> S3Request<DeleteObjectTaggingInput> {
        S3Request {
            input: DeleteObjectTaggingInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id,
                ..Default::default()
            },
            method: http::Method::DELETE,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn delete_objects_req(bucket: &str, keys: Vec<&str>) -> S3Request<DeleteObjectsInput> {
        S3Request {
            input: DeleteObjectsInput {
                bucket: bucket.to_string(),
                bypass_governance_retention: None,
                checksum_algorithm: None,
                delete: s3s::dto::Delete {
                    objects: keys
                        .into_iter()
                        .map(|key| s3s::dto::ObjectIdentifier {
                            e_tag: None,
                            key: key.to_string(),
                            last_modified_time: None,
                            size: None,
                            version_id: None,
                        })
                        .collect(),
                    quiet: None,
                },
                expected_bucket_owner: None,
                mfa: None,
                request_payer: None,
            },
            method: http::Method::POST,
            uri: format!("/{bucket}").parse().expect("uri"),
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

    fn list_req(bucket: &str, max_keys: Option<i32>) -> S3Request<ListObjectsInput> {
        S3Request {
            input: ListObjectsInput {
                bucket: bucket.to_string(),
                max_keys,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/{bucket}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn list_v2_req(bucket: &str, max_keys: Option<i32>) -> S3Request<ListObjectsV2Input> {
        S3Request {
            input: ListObjectsV2Input {
                bucket: bucket.to_string(),
                max_keys,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/{bucket}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn list_versions_req(
        bucket: &str,
        max_keys: Option<i32>,
    ) -> S3Request<ListObjectVersionsInput> {
        S3Request {
            input: ListObjectVersionsInput {
                bucket: bucket.to_string(),
                max_keys,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/{bucket}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn list_versions_paged_req(
        bucket: &str,
        max_keys: i32,
        delimiter: Option<&str>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
    ) -> S3Request<ListObjectVersionsInput> {
        S3Request {
            input: ListObjectVersionsInput {
                bucket: bucket.to_string(),
                max_keys: Some(max_keys),
                delimiter: delimiter.map(str::to_string),
                key_marker,
                version_id_marker,
                ..Default::default()
            },
            method: http::Method::GET,
            uri: format!("/{bucket}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// Make every `get_bucket_versioning` query fail while leaving the rest of
    /// the schema readable.
    ///
    /// `get_bucket_versioning` reads `s3_bucket`, so renaming that table away
    /// turns the versioning lookup into a hard SQL error while object reads
    /// (which touch `s3_object`) keep succeeding. That isolates the exact
    /// condition under test: the *query itself* fails, so a handler that
    /// reports the bucket as unversioned is provably swallowing an error
    /// rather than observing `Ok(None)`.
    ///
    /// The table is renamed rather than dropped because `s3_object` holds a
    /// foreign key into it and SQLite refuses to drop a referenced parent.
    async fn break_versioning_lookup(svc: &TeleS3<Memory<1, 1>>) {
        svc.repo
            .db
            .execute_unprepared("ALTER TABLE s3_bucket RENAME TO s3_bucket_hidden")
            .await
            .expect("rename s3_bucket away");
    }

    /// A versioning lookup that fails must surface, not read as "unversioned".
    ///
    /// Before the fix each of these five sites did
    /// `.unwrap_or(None)`, so a broken versioning query was indistinguishable
    /// from a genuinely unversioned bucket -- and on the destructive paths
    /// (bulk delete, tagging write) that meant silently discarding versions.
    #[tokio::test]
    async fn versioned_read_paths_propagate_versioning_lookup_failure() {
        let svc = test_service().await;
        seed_versions(&svc, &["k"]).await;
        break_versioning_lookup(&svc).await;

        let err = svc
            .get_object_inner(get_req("b", "k", None))
            .await
            .expect_err("get must not swallow a failed versioning lookup");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let err = svc
            .head_object_inner(head_req("b", "k", None))
            .await
            .expect_err("head must not swallow a failed versioning lookup");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let err = svc
            .get_object_tagging_inner(get_tagging_req("b", "k", None))
            .await
            .expect_err("get tagging must not swallow a failed versioning lookup");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    /// The destructive sites: a failed versioning lookup must abort the whole
    /// request rather than fall through to the non-versioned path.
    #[tokio::test]
    async fn destructive_paths_fail_closed_on_versioning_lookup_failure() {
        let svc = test_service().await;
        seed_versions(&svc, &["k"]).await;
        break_versioning_lookup(&svc).await;

        let err = svc
            .delete_objects_inner(delete_objects_req("b", vec!["k"]))
            .await
            .expect_err("bulk delete must not fall through to the unversioned path");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let err = svc
            .put_object_tagging_inner(put_tagging_req("b", "k", None))
            .await
            .expect_err("tagging write must not swallow a failed versioning lookup");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let err = svc
            .delete_object_tagging_inner(delete_tagging_req("b", "k", None))
            .await
            .expect_err("tagging delete must not swallow a failed versioning lookup");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    /// The unversioned case must keep answering `Ok(None)`: the fix changes
    /// error handling only, never a bucket that genuinely has no versioning.
    #[tokio::test]
    async fn unversioned_bucket_still_reports_no_version_id() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.repo
            .cas_put_object(
                "b".into(),
                "k".into(),
                super::super::repo::ObjectWrite {
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

        let res = svc
            .head_object_inner(head_req("b", "k", None))
            .await
            .expect("unversioned head must succeed");
        assert_eq!(res.output.version_id, None);
    }

    /// Seed a versioned bucket with one version per key.
    async fn seed_versions(svc: &TeleS3<Memory<1, 1>>, keys: &[&str]) {
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.repo
            .put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");
        for key in keys {
            svc.repo
                .cas_put_object(
                    "b".into(),
                    (*key).to_string(),
                    super::super::repo::ObjectWrite {
                        size: 1,
                        content_type: None,
                        etag: Some(format!("etag-{key}")),
                        content: serde_json::json!({"item": []}),
                        user_metadata: serde_json::json!({}),
                        checksums: serde_json::json!({}),
                        tags: serde_json::json!([]),
                    },
                    super::super::repo::PutCondition::None,
                )
                .await
                .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
        }
    }

    /// One page of `list_object_versions_inner` reduced to the fields a
    /// delimiter-aware client actually reads.
    #[derive(Debug, PartialEq, Eq)]
    struct Page {
        prefixes: Vec<String>,
        keys: Vec<String>,
        is_truncated: Option<bool>,
        next_key_marker: Option<String>,
        next_version_id_marker: Option<String>,
    }

    async fn versions_page(
        svc: &TeleS3<Memory<1, 1>>,
        max_keys: i32,
        delimiter: Option<&str>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
    ) -> Page {
        let res = svc
            .list_object_versions_inner(list_versions_paged_req(
                "b",
                max_keys,
                delimiter,
                key_marker,
                version_id_marker,
            ))
            .await
            .expect("list versions");
        let out = res.output;
        Page {
            prefixes: out
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .map(|p| p.prefix.expect("prefix"))
                .collect(),
            keys: out
                .versions
                .unwrap_or_default()
                .into_iter()
                .map(|v| v.key.expect("key"))
                .collect(),
            is_truncated: out.is_truncated,
            next_key_marker: out.next_key_marker,
            next_version_id_marker: out.next_version_id_marker,
        }
    }

    /// Walk every page of a listing, following the continuation markers.
    async fn walk_versions(
        svc: &TeleS3<Memory<1, 1>>,
        max_keys: i32,
        delimiter: Option<&str>,
    ) -> Vec<Page> {
        let mut pages = Vec::new();
        let (mut km, mut vid) = (None, None);
        loop {
            let page = versions_page(svc, max_keys, delimiter, km.clone(), vid.clone()).await;
            let truncated = page.is_truncated == Some(true);
            pages.push(page);
            if !truncated {
                return pages;
            }
            assert!(
                pages.len() < 10,
                "continuation markers must advance; walk did not terminate"
            );
            km = pages.last().and_then(|p| p.next_key_marker.clone());
            vid = pages.last().and_then(|p| p.next_version_id_marker.clone());
        }
    }

    #[tokio::test]
    async fn delimiter_rollup_is_unaffected_by_sql_page_limit() {
        let svc = test_service().await;
        seed_versions(&svc, &["a/1", "a/2", "b/1"]).await;

        // The unbounded baseline: every key fits in one page.
        let whole = versions_page(&svc, 1000, Some("/"), None, None).await;
        assert_eq!(whole.prefixes, ["a/", "b/"]);
        assert!(whole.keys.is_empty());
        assert_eq!(whole.is_truncated, Some(false));

        // Paged below the result set: same rollup, split across pages by the
        // handler's existing fold. The `CommonPrefix` entries per page are
        // pinned to current (pre-SQL-cap) behaviour.
        let paged = walk_versions(&svc, 1, Some("/")).await;
        assert_eq!(
            paged.len(),
            3,
            "three keys must take three pages: {paged:?}"
        );
        assert_eq!(paged[0].prefixes, ["a/"]);
        assert_eq!(paged[1].prefixes, ["a/"]);
        assert_eq!(paged[2].prefixes, ["b/"]);
        assert_eq!(paged[2].is_truncated, Some(false));
        for page in &paged[..2] {
            assert_eq!(page.is_truncated, Some(true), "{page:?}");
            assert!(page.next_key_marker.is_some(), "{page:?}");
            assert!(page.next_version_id_marker.is_some(), "{page:?}");
        }
        assert_eq!(paged[2].next_key_marker, None);
        assert_eq!(paged[2].next_version_id_marker, None);

        // Flat (no delimiter) walk: every key exactly once, in order.
        let flat = walk_versions(&svc, 1, None).await;
        assert_eq!(
            flat.iter().map(|p| p.keys.clone()).collect::<Vec<_>>(),
            [["a/1"], ["a/2"], ["b/1"]]
        );
        assert!(
            flat.iter().all(|p| p.prefixes.is_empty()),
            "no delimiter must mean no rollup: {flat:?}"
        );
    }

    #[tokio::test]
    async fn version_pages_cover_every_row_exactly_once() {
        let svc = test_service().await;
        seed_versions(&svc, &["a/1", "a/2", "a/3", "b/1", "b/2", "c/1"]).await;

        let flat = walk_versions(&svc, 2, None).await;
        assert_eq!(
            flat.iter().flat_map(|p| p.keys.clone()).collect::<Vec<_>>(),
            ["a/1", "a/2", "a/3", "b/1", "b/2", "c/1"],
            "SQL-limited pages must neither skip nor repeat a key"
        );
        assert_eq!(flat.last().expect("last page").is_truncated, Some(false));
    }

    #[tokio::test]
    async fn version_max_keys_of_one_pages_a_single_key() {
        let svc = test_service().await;
        seed_versions(&svc, &["a/1", "b/1"]).await;

        let page = versions_page(&svc, 1, None, None, None).await;
        assert_eq!(page.keys, ["a/1"]);
        assert_eq!(page.is_truncated, Some(true));
        assert_eq!(page.next_key_marker.as_deref(), Some("a/1"));
        assert!(page.next_version_id_marker.is_some());
    }

    fn put_req(bucket: &str, key: &str, content_length: Option<i64>) -> S3Request<PutObjectInput> {
        put_body_req(bucket, key, content_length, Vec::new())
    }

    /// A `PutObject` carrying `body`, so the write actually reaches the
    /// backend (a zero-length body never calls it).
    fn put_body_req(
        bucket: &str,
        key: &str,
        content_length: Option<i64>,
        body: Vec<u8>,
    ) -> S3Request<PutObjectInput> {
        S3Request {
            input: PutObjectInput {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_length,
                body: Some(StreamingBlob::from_bytes(bytes::Bytes::from(body))),
                ..Default::default()
            },
            method: http::Method::PUT,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// A `PutObject` carrying `body` and a client `x-amz-checksum-crc32`.
    fn put_body_with_crc32_req(
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        checksum_crc32: &str,
    ) -> S3Request<PutObjectInput> {
        let mut req = put_body_req(bucket, key, Some(body.len() as i64), body);
        req.input.checksum_crc32 = Some(checksum_crc32.to_string());
        req
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

    #[tokio::test]
    async fn a_successful_write_records_and_resolves_a_publish_intent() {
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        svc.put_object_inner(put_body_req("b", "k", Some(3), b"abc".to_vec()))
            .await
            .expect("put");

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "exactly one intent per write: {rows:?}");

        let row = &rows[0];
        assert_eq!(row.bucket_id, "b");
        assert_eq!(row.object_id, "k");
        assert_eq!(row.size, 3);
        assert!(
            row.resolved_at.is_some(),
            "a confirmed publish must resolve its intent: {row:?}"
        );
        assert!(
            row.blob_id.is_some(),
            "a resolved intent records the backend key: {row:?}"
        );
    }

    #[tokio::test]
    async fn a_failed_write_leaves_its_publish_intent_pending() {
        // The load-bearing assertion of this scope: a publish that may have
        // happened and returned no key is found by nothing else, so a failed
        // write must not clear the only record of it. A reconciler that finds
        // no pending intent has nothing to repair.
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.backend.fail_writes(true);

        svc.put_object_inner(put_body_req("b", "k", Some(3), b"abc".to_vec()))
            .await
            .expect_err("a refused write must fail the request");

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "the intent outlives the failure: {rows:?}");

        let row = &rows[0];
        assert!(
            row.resolved_at.is_none(),
            "a failed write must leave the intent pending: {row:?}"
        );
        assert!(
            row.blob_id.is_none(),
            "no key was returned, so none can be recorded: {row:?}"
        );
        assert_eq!(row.bucket_id, "b");
        assert_eq!(row.object_id, "k");
    }

    #[tokio::test]
    async fn a_failed_intent_resolve_does_not_publish_the_object() {
        // The resolve is part of the publish transaction. If it fails, the
        // object must not commit while its intent stays pending: the reclaim
        // pass would later delete the bytes of a live object.
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        refuse_publish_intent_updates(&svc).await;

        let err = svc
            .put_object_inner(put_body_req("b", "k", Some(3), b"abc".to_vec()))
            .await
            .expect_err("a failed resolve must fail the write");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        assert!(
            !svc.repo.object_exists("b", "k").await.expect("exists"),
            "the failed resolve must roll back the object row"
        );
        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "the intent row must survive: {rows:?}");
        assert!(
            rows[0].resolved_at.is_none(),
            "a failed resolve must leave the intent pending: {rows:?}"
        );
    }

    #[tokio::test]
    async fn a_checksum_failure_keeps_the_intent_when_the_backend_delete_fails() {
        // A failed delete leaves bytes with no metadata row. The pending
        // intent is then the only record that can lead the reclaim pass back
        // to them, so it must not be cleared.
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.backend.fail_deletes(true);

        let err = svc
            .put_object_inner(put_body_with_crc32_req(
                "b",
                "k",
                b"abc".to_vec(),
                "AAAAAA==",
            ))
            .await
            .expect_err("a mismatching checksum must fail the write");
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "the intent row must survive: {rows:?}");
        assert!(
            rows[0].resolved_at.is_none(),
            "a failed delete must keep the intent pending: {rows:?}"
        );
    }

    #[tokio::test]
    async fn an_invalid_content_md5_keeps_the_intent_when_the_backend_delete_fails() {
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        svc.backend.fail_deletes(true);

        let mut req = put_body_req("b", "k", Some(3), b"abc".to_vec());
        req.input.content_md5 = Some("not base64!".into());

        let err = svc
            .put_object_inner(req)
            .await
            .expect_err("an undecodable content-md5 must fail the write");
        assert_eq!(*err.code(), S3ErrorCode::InvalidDigest);

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1, "the intent row must survive: {rows:?}");
        assert!(
            rows[0].resolved_at.is_none(),
            "a failed delete must keep the intent pending: {rows:?}"
        );
    }

    #[tokio::test]
    async fn a_checksum_failure_clears_the_intent_after_the_backend_delete() {
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let err = svc
            .put_object_inner(put_body_with_crc32_req(
                "b",
                "k",
                b"abc".to_vec(),
                "AAAAAA==",
            ))
            .await
            .expect_err("a mismatching checksum must fail the write");
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);

        assert!(
            intents(&svc).await.is_empty(),
            "a confirmed delete settles the publish, so no intent must remain"
        );
    }

    #[tokio::test]
    async fn a_zero_size_write_records_no_intent() {
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        svc.put_object_inner(put_req("b", "k", Some(0)))
            .await
            .expect("put an empty object");

        assert!(
            intents(&svc).await.is_empty(),
            "an empty body never calls the backend, so it has no publish to record"
        );
    }

    #[tokio::test]
    async fn a_publish_id_carries_no_user_data() {
        // The identifier is published under in the backend, so it must never
        // embed the bucket or key the object was written for.
        let svc = flaky_write_service().await;
        svc.repo
            .create_bucket("customer-private".into(), None)
            .await
            .expect("create bucket");

        svc.put_object_inner(put_body_req(
            "customer-private",
            "invoices/q3.pdf",
            Some(3),
            b"abc".to_vec(),
        ))
        .await
        .expect("put");

        let rows = intents(&svc).await;
        assert_eq!(rows.len(), 1);

        let publish_id = &rows[0].publish_id;
        assert!(
            !publish_id.contains("customer-private"),
            "the publish identifier leaks the bucket: {publish_id}"
        );
        assert!(
            !publish_id.contains("invoices/q3.pdf"),
            "the publish identifier leaks the key: {publish_id}"
        );
    }

    #[tokio::test]
    async fn negative_max_keys_is_invalid_argument() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let err = svc
            .list_objects_inner(list_req("b", Some(-1)))
            .await
            .expect_err("negative max-keys must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = svc
            .list_objects_v2_inner(list_v2_req("b", Some(-1)))
            .await
            .expect_err("negative max-keys must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = svc
            .list_object_versions_inner(list_versions_req("b", Some(-1)))
            .await
            .expect_err("negative max-keys must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn max_keys_above_ceiling_is_clamped() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let res = svc
            .list_objects_inner(list_req("b", Some(5000)))
            .await
            .expect("oversized max-keys must be clamped, not rejected");
        assert_eq!(res.output.max_keys, Some(1000));

        let res = svc
            .list_objects_v2_inner(list_v2_req("b", Some(5000)))
            .await
            .expect("oversized max-keys must be clamped, not rejected");
        assert_eq!(res.output.max_keys, Some(1000));

        let res = svc
            .list_object_versions_inner(list_versions_req("b", Some(5000)))
            .await
            .expect("oversized max-keys must be clamped, not rejected");
        assert_eq!(res.output.max_keys, Some(1000));
    }

    #[tokio::test]
    async fn default_max_keys_is_the_ceiling() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let res = svc
            .list_objects_inner(list_req("b", None))
            .await
            .expect("unset max-keys defaults to the ceiling");
        assert_eq!(res.output.max_keys, Some(1000));
    }

    #[tokio::test]
    async fn negative_content_length_is_invalid_argument() {
        let svc = test_service().await;
        svc.repo
            .create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let err = svc
            .put_object_inner(put_req("b", "k", Some(-1)))
            .await
            .expect_err("negative content-length must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);

        let err = svc
            .put_object_inner(put_req("b", "k", None))
            .await
            .expect_err("missing content-length must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::MissingContentLength);
    }

    fn rows<'a>(keys: &'a [&'a str]) -> Vec<ListRow<'a>> {
        let stamp = chrono::Utc::now();
        keys.iter()
            .map(|key| (*key, key.len() as u32, stamp))
            .collect()
    }

    fn listed_keys(objects: &[Object]) -> Vec<&str> {
        objects
            .iter()
            .filter_map(|object| object.key.as_deref())
            .collect()
    }

    fn listed_prefixes(prefixes: &[CommonPrefix]) -> Vec<&str> {
        prefixes
            .iter()
            .filter_map(|common| common.prefix.as_deref())
            .collect()
    }

    #[test]
    fn common_prefix_splits_at_the_first_delimiter_after_the_prefix() {
        assert_eq!(
            common_prefix_for("p/a/1", "p/", "/").as_deref(),
            Some("p/a/")
        );
        assert_eq!(common_prefix_for("p/b", "p/", "/"), None);
    }

    #[test]
    fn common_prefix_splits_the_whole_key_when_the_prefix_does_not_match() {
        // Rows reaching the rollup have already been filtered by the prefix,
        // but the split falls back to the key itself while the listed prefix
        // is still what the common prefix is rebuilt from.
        assert_eq!(common_prefix_for("a/b", "p/", "/").as_deref(), Some("p/a/"));
        assert_eq!(common_prefix_for("abc", "p/", "/"), None);
    }

    #[test]
    fn rollup_without_delimiter_lists_every_row_as_an_object() {
        let (contents, prefixes, next) = rollup_list_page(rows(&["a/1", "b"]), "", None, false);

        assert_eq!(listed_keys(&contents), vec!["a/1", "b"]);
        assert_eq!(contents[0].size, Some(3));
        assert!(prefixes.is_empty());
        assert_eq!(next, None);
    }

    #[test]
    fn rollup_with_delimiter_rolls_shared_prefixes_out_of_the_contents() {
        let (contents, prefixes, _) = rollup_list_page(
            rows(&["photos/1.jpg", "photos/2.jpg", "docs/readme"]),
            "",
            Some("/"),
            false,
        );

        assert!(contents.is_empty());
        // Known quirk, preserved as-is: the rollup is per row, so a prefix
        // shared by several rows is emitted once per row.
        assert_eq!(
            listed_prefixes(&prefixes),
            vec!["photos/", "photos/", "docs/"]
        );
    }

    #[test]
    fn rollup_treats_a_prefix_that_is_also_a_key_as_a_common_prefix() {
        let (contents, prefixes, _) =
            rollup_list_page(rows(&["a", "a/", "a/b"]), "", Some("/"), false);

        assert_eq!(listed_keys(&contents), vec!["a"]);
        assert_eq!(listed_prefixes(&prefixes), vec!["a/", "a/"]);
    }

    async fn seed_unversioned_object(svc: &TeleS3<Memory<1, 1>>, bucket: &str, key: &str) {
        svc.repo
            .create_bucket(bucket.to_string(), None)
            .await
            .expect("create bucket");
        svc.repo
            .cas_put_object(
                bucket.to_string(),
                key.to_string(),
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
    }

    fn copy_req(bucket: &str, key: &str, src_key: &str) -> S3Request<CopyObjectInput> {
        // `CopyObjectInput` has no `Default`, so build it through the dto
        // builder rather than spelling out all thirty optional fields.
        let mut builder = CopyObjectInput::builder();
        builder
            .set_bucket(bucket.to_string())
            .set_key(key.to_string())
            .set_copy_source(CopySource::Bucket {
                bucket: bucket.to_string().into(),
                key: src_key.to_string().into(),
                version_id: None,
            });
        let input = builder.build().expect("copy request");

        S3Request {
            input,
            method: http::Method::PUT,
            uri: format!("/{bucket}/{key}").parse().expect("uri"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::default(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    /// A read failure on the object a copy is about to replace must abort the
    /// request, not silently skip releasing the old object's blobs.
    ///
    /// Before the fix this lookup read `else if let Ok(old) = ...get_object()`,
    /// so a repository failure was indistinguishable from "there is nothing to
    /// replace" and the old object's blob references were never released.
    ///
    /// The assertion is made on `replaced_object` rather than on the whole
    /// request because a table-level fault cannot be isolated to this lookup:
    /// the copy's *source* read hits `s3_object` too, so a broken table fails
    /// the request earlier no matter how this site is written. The helper is
    /// the exact expression both write paths evaluate, and the end-to-end
    /// absence test below pins the branch that follows it.
    #[tokio::test]
    async fn replaced_object_propagates_a_repository_read_failure() {
        let svc = test_service().await;
        seed_unversioned_object(&svc, "b", "old").await;
        svc.repo
            .db
            .execute_unprepared("ALTER TABLE s3_object RENAME TO s3_object_hidden")
            .await
            .expect("rename s3_object away");

        let err = svc
            .replaced_object("b", "old")
            .await
            .expect_err("a failed replaced-object read must not be swallowed");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    /// The absent case, at both levels: a key with nothing to replace is `None`
    /// from the helper and a successful copy at request level.
    ///
    /// `get_object` signals absence as `Err(NoSuchKey)`, so this is the guard
    /// against over-correcting the fix into a blanket `?` that would break
    /// copying onto a fresh key.
    #[tokio::test]
    async fn replaced_object_is_none_for_an_absent_key_and_copy_still_succeeds() {
        let svc = test_service().await;
        seed_unversioned_object(&svc, "b", "src").await;

        assert!(
            svc.replaced_object("b", "dst")
                .await
                .expect("an absent key is not an error")
                .is_none()
        );

        let res = svc
            .copy_object_inner(copy_req("b", "dst", "src"))
            .await
            .expect("copying onto a key that holds nothing is a normal first write");
        assert!(
            res.output.copy_object_result.is_some(),
            "an embedded copy result marks a successful copy"
        );

        // The key now holds an object, so the same lookup reports it.
        assert!(
            svc.replaced_object("b", "dst")
                .await
                .expect("a present key is not an error")
                .is_some()
        );
    }

    #[test]
    fn rollup_applies_the_listed_prefix_before_the_delimiter() {
        let (contents, prefixes, _) =
            rollup_list_page(rows(&["p/a/1", "p/a/2", "p/b"]), "p/", Some("/"), false);

        assert_eq!(listed_keys(&contents), vec!["p/b"]);
        assert_eq!(listed_prefixes(&prefixes), vec!["p/a/", "p/a/"]);
    }

    #[test]
    fn rollup_of_an_empty_page_is_empty_and_uncursored() {
        let (contents, prefixes, next) = rollup_list_page(Vec::new(), "", Some("/"), false);

        assert!(contents.is_empty());
        assert!(prefixes.is_empty());
        assert_eq!(next, None);
    }

    #[test]
    fn rollup_cursor_is_the_last_row_key_only_when_the_page_was_truncated() {
        let (contents, prefixes, next) = rollup_list_page(rows(&["a", "b"]), "", None, true);

        assert_eq!(listed_keys(&contents), vec!["a", "b"]);
        assert!(prefixes.is_empty());
        assert_eq!(next.as_deref(), Some("b"));

        let (_, _, next) = rollup_list_page(rows(&["a", "b"]), "", None, false);
        assert_eq!(next, None);
    }
}
