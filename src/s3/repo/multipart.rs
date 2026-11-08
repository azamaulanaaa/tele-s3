use std::{collections::BTreeMap, ops::Bound};

use redb::{ReadableDatabase, ReadableTable};
use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::instrument;

use super::blobs::release_blob_refs_in_txn;
use super::{ObjectWrite, PutCondition, Repository, StoreResultExt};
use crate::s3::objects::checked_max_uploads;
use crate::s3::types::MultipartUploadPart;
use crate::storage::{Fault, MULTIPART, MultipartUploadRow, StoreError, decode, encode};

/// Field bundle for writing an in-progress multipart upload's state row.
///
/// Three of these are positional JSON payloads, so a swapped pair would
/// compile cleanly and silently corrupt the upload. Naming the fields keeps
/// every call site self-checking.
#[derive(Debug)]
pub(crate) struct MultipartUploadStateUpdate {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: String,
    pub(crate) content_type: Option<String>,
    /// x-amz-meta-* map as a JSON object. Empty object when none.
    pub(crate) user_metadata: serde_json::Value,
    /// Object tag-set as a JSON array of {key, value}. Empty array when none.
    pub(crate) tags: serde_json::Value,
    pub(crate) content: serde_json::Value,
}

/// Field bundle for completing one upload: the upload identity, the content
/// the caller built the object from, the object write, and the references
/// the completion retires.
pub(crate) struct MultipartCompletion {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: String,
    /// Raw state content the object was built from.
    pub(crate) expected_content: serde_json::Value,
    pub(crate) data: ObjectWrite,
    pub(crate) condition: PutCondition,
    /// Parts left out of the object and, for an unversioned bucket, the
    /// object it replaces.
    pub(crate) release_ids: Vec<String>,
}

fn multipart_row<T>(
    table: &T,
    bucket: &str,
    key: &str,
    upload_id: &str,
) -> Result<Option<MultipartUploadRow>, StoreError>
where
    T: ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    let Some(value) = table.get((bucket, key, upload_id))? else {
        return Ok(None);
    };

    Ok(Some(decode(value.value())?))
}

impl Repository {
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn upsert_multipart_upload_state(
        &self,
        update: MultipartUploadStateUpdate,
    ) -> S3Result<()> {
        let MultipartUploadStateUpdate {
            bucket,
            key,
            upload_id,
            content_type,
            user_metadata,
            tags,
            content,
        } = update;

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(MULTIPART).store()?;
                    let existing = multipart_row(&table, &bucket, &key, &upload_id).store()?;
                    let row = match existing {
                        Some(mut row) => {
                            row.content_type = content_type;
                            row.tags = Some(tags);
                            row.content = content;
                            row
                        }
                        None => MultipartUploadRow {
                            bucket_id: bucket.clone(),
                            object_id: key.clone(),
                            upload_id: upload_id.clone(),
                            content_type,
                            user_metadata,
                            tags: Some(tags),
                            content,
                        },
                    };
                    let bytes = encode(&row).store()?;
                    table
                        .insert(
                            (bucket.as_str(), key.as_str(), upload_id.as_str()),
                            bytes.as_slice(),
                        )
                        .store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// List one page of the bucket's in-flight multipart uploads.
    ///
    /// Rows come back ordered `ObjectId ASC, UploadId ASC` and the page is
    /// bounded by the caller's limit, so a paginated walk never materialises
    /// the whole table per page.
    ///
    /// The `(key_marker, upload_id_marker)` boundary is a range start rather
    /// than a post-fetch filter. With a `key_marker` alone the cursor skips
    /// the whole key. With an `upload_id_marker` it resumes after that exact
    /// row.
    ///
    /// `max_uploads` is validated (negatives rejected, values above the S3
    /// ceiling clamped) and the code fetches `max_uploads + 1` rows: the
    /// returned bool is the truncation sentinel and the returned vector holds
    /// at most `max_uploads` rows.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: Option<i32>,
    ) -> S3Result<(Vec<MultipartUploadRow>, bool)> {
        let max_uploads = checked_max_uploads(max_uploads)?;
        let limit =
            usize::try_from(max_uploads).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
        let fetch_max = limit.saturating_add(1);

        let bucket = bucket.to_owned();
        let prefix = prefix.map(str::to_owned);
        let key_marker = key_marker.map(str::to_owned);
        let upload_id_marker = upload_id_marker.map(str::to_owned);

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(MULTIPART).store()?;

                let start = match (key_marker.as_deref(), upload_id_marker.as_deref()) {
                    (Some(key_marker), Some(upload_id_marker)) => {
                        Bound::Excluded((bucket.as_str(), key_marker, upload_id_marker))
                    }
                    (Some(key_marker), None) => Bound::Included((bucket.as_str(), key_marker, "")),
                    _ => Bound::Included((bucket.as_str(), "", "")),
                };
                let mut rows = Vec::new();

                for entry in table.range((start, Bound::Unbounded)).store()? {
                    let (key, value) = entry.store()?;
                    let (entry_bucket, key, _) = key.value();
                    if entry_bucket != bucket {
                        break;
                    }
                    if let Some(key_marker) = key_marker.as_deref()
                        && upload_id_marker.is_none()
                        && key == key_marker
                    {
                        continue;
                    }
                    if let Some(prefix) = prefix.as_deref()
                        && !key.starts_with(prefix)
                    {
                        if key < prefix {
                            continue;
                        }
                        break;
                    }

                    rows.push(decode(value.value()).store()?);
                    if rows.len() == fetch_max {
                        break;
                    }
                }

                let is_truncated = rows.len() > limit;
                if is_truncated {
                    rows.truncate(limit);
                }

                Ok((rows, is_truncated))
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn get_bucket_upload_count(&self, bucket: &str) -> S3Result<u64> {
        let bucket = bucket.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(MULTIPART).store()?;
                let mut count = 0_u64;

                for entry in table.range((bucket.as_str(), "", "")..).store()? {
                    let (key, _) = entry.store()?;
                    if key.value().0 != bucket {
                        break;
                    }
                    count += 1;
                }

                Ok(count)
            })
            .await
    }

    pub(crate) async fn get_multipart_upload_state(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> S3Result<MultipartUploadRow> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let upload_id = upload_id.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(MULTIPART).store()?;

                multipart_row(&table, &bucket, &key, &upload_id)
                    .store()?
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))
            })
            .await
    }

    /// Delete one upload state without changing its blob references.
    ///
    /// Callers that must keep the state and its references together use
    /// [`Self::delete_multipart_upload_state_releasing_blobs`] instead.
    #[allow(dead_code)]
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn delete_multipart_upload_state(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> S3Result<Option<MultipartUploadRow>> {
        self.inject(Fault::StateDelete)?;
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let upload_id = upload_id.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let row = {
                    let mut table = txn.open_table(MULTIPART).store()?;
                    let row = multipart_row(&table, &bucket, &key, &upload_id).store()?;
                    table
                        .remove((bucket.as_str(), key.as_str(), upload_id.as_str()))
                        .store()?;
                    row
                };
                txn.commit().store()?;

                Ok(row)
            })
            .await
    }

    /// Delete one upload state and drop its blob references in one
    /// transaction.
    ///
    /// `extract_ids` turns the stored content into the blob ids that the
    /// upload owns. If the state is absent, the result is `None` and nothing
    /// changes. If any step fails, the transaction rolls back, so the state
    /// and its references stay together for a retry.
    #[instrument(skip(self, extract_ids), level = "debug", err)]
    pub(crate) async fn delete_multipart_upload_state_releasing_blobs<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        extract_ids: F,
    ) -> S3Result<Option<Vec<String>>>
    where
        F: FnOnce(&serde_json::Value) -> S3Result<Vec<String>> + Send + 'static,
    {
        self.inject(Fault::StateDelete)?;
        self.inject(Fault::BlobRelease)?;
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let upload_id = upload_id.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let result = (|| {
                    let row = {
                        let table = txn.open_table(MULTIPART).store()?;
                        multipart_row(&table, &bucket, &key, &upload_id).store()?
                    };
                    let Some(row) = row else {
                        return Ok(None);
                    };

                    let ids = extract_ids(&row.content)?;
                    let released = release_blob_refs_in_txn(&txn, &ids)?;
                    {
                        let mut table = txn.open_table(MULTIPART).store()?;
                        table
                            .remove((bucket.as_str(), key.as_str(), upload_id.as_str()))
                            .store()?;
                    }

                    Ok(Some(released))
                })();

                match result {
                    Ok(output) => {
                        txn.commit().store()?;
                        Ok(output)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    /// Publish a completed object and delete its upload state in one
    /// transaction.
    ///
    /// The caller builds the object from `expected_content`. The transaction
    /// checks that the stored content still equals it. A missing or changed
    /// state rolls the object write back.
    ///
    /// `release_ids` are the references the completion retires. They are
    /// released in the same transaction, so a failure cannot publish the
    /// object while leaving an old reference behind.
    #[instrument(skip(self, completion), level = "debug", err)]
    pub(crate) async fn complete_multipart_upload(
        &self,
        completion: MultipartCompletion,
    ) -> S3Result<(String, Vec<String>)> {
        self.inject(Fault::StateDelete)?;
        self.inject(Fault::BlobRelease)?;
        let MultipartCompletion {
            bucket,
            key,
            upload_id,
            expected_content,
            data,
            condition,
            release_ids,
        } = completion;

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let result = (|| {
                    let model = {
                        let table = txn.open_table(MULTIPART).store()?;
                        multipart_row(&table, &bucket, &key, &upload_id).store()?
                    }
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

                    if model.content != expected_content {
                        return Err(S3Error::new(S3ErrorCode::InternalError));
                    }

                    let version_id =
                        super::objects::cas_put_object_txn(&txn, &bucket, &key, data, condition)?;

                    {
                        let mut table = txn.open_table(MULTIPART).store()?;
                        let removed = table
                            .remove((bucket.as_str(), key.as_str(), upload_id.as_str()))
                            .store()?;
                        if removed.is_none() {
                            return Err(S3Error::new(S3ErrorCode::InternalError));
                        }
                    }

                    let released = release_blob_refs_in_txn(&txn, &release_ids)?;

                    Ok((version_id, released))
                })();

                match result {
                    Ok(output) => {
                        txn.commit().store()?;
                        Ok(output)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    /// Store one part and release the references of the part it replaces in
    /// one transaction.
    ///
    /// Re-uploading a part number replaces the old metadata. Without this
    /// transaction the old blob would lose its holder but keep its reference,
    /// so a retried or repeated upload would leak one blob per attempt.
    #[instrument(skip(self, part), level = "debug", err)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn set_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part: MultipartUploadPart,
        resolve: Option<(&str, &str)>,
    ) -> S3Result<Vec<String>> {
        self.inject(Fault::BlobRelease)?;
        if resolve.is_some() {
            self.inject(Fault::PublishIntentUpdate)?;
        }
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let upload_id = upload_id.to_owned();
        let resolve =
            resolve.map(|(publish_id, blob_id)| (publish_id.to_owned(), blob_id.to_owned()));

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let result = (|| {
                    let model = {
                        let table = txn.open_table(MULTIPART).store()?;
                        multipart_row(&table, &bucket, &key, &upload_id).store()?
                    }
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

                    let mut parts: BTreeMap<i32, MultipartUploadPart> =
                        serde_json::from_value(model.content.clone()).store()?;

                    let previous = parts.insert(part_number, part.clone());
                    let new_content = serde_json::to_value(&parts).store()?;

                    if new_content != model.content {
                        let mut table = txn.open_table(MULTIPART).store()?;
                        let mut row = model.clone();
                        row.content = new_content;
                        let bytes = encode(&row).store()?;
                        table
                            .insert(
                                (bucket.as_str(), key.as_str(), upload_id.as_str()),
                                bytes.as_slice(),
                            )
                            .store()?;
                    }

                    let released = match previous {
                        Some(previous) => {
                            let ids: Vec<String> = previous
                                .metadata_items
                                .into_iter()
                                .map(|item| item.id)
                                .collect();
                            release_blob_refs_in_txn(&txn, &ids)?
                        }
                        None => Vec::new(),
                    };

                    if let Some((publish_id, blob_id)) = resolve {
                        super::blobs::resolve_publish_intent_in_txn(&txn, &publish_id, blob_id)?;
                    }

                    Ok(released)
                })();

                match result {
                    Ok(released) => {
                        txn.commit().store()?;
                        Ok(released)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    #[allow(dead_code)]
    #[instrument(skip(self, action), level = "debug", err)]
    pub(crate) async fn cas_update_multipart_content<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()> + Send + 'static,
    {
        self.cas_update_multipart_content_inner(bucket, key, upload_id, None, action)
            .await
    }

    /// Commit a part update and resolve its publish intent in one transaction.
    #[allow(dead_code)]
    #[instrument(skip(self, action), level = "debug", err)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn cas_update_multipart_content_and_resolve<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        publish_id: &str,
        blob_id: &str,
        action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()> + Send + 'static,
    {
        self.inject(Fault::PublishIntentUpdate)?;
        self.cas_update_multipart_content_inner(
            bucket,
            key,
            upload_id,
            Some((publish_id.to_owned(), blob_id.to_owned())),
            action,
        )
        .await
    }

    async fn cas_update_multipart_content_inner<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        resolve: Option<(String, String)>,
        mut action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()> + Send + 'static,
    {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let upload_id = upload_id.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let result = (|| {
                    let model = {
                        let table = txn.open_table(MULTIPART).store()?;
                        multipart_row(&table, &bucket, &key, &upload_id).store()?
                    }
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

                    let mut new_content = model.content.clone();
                    action(&mut new_content)?;

                    if new_content != model.content {
                        let mut table = txn.open_table(MULTIPART).store()?;
                        let mut row = model.clone();
                        row.content = new_content;
                        let bytes = encode(&row).store()?;
                        table
                            .insert(
                                (bucket.as_str(), key.as_str(), upload_id.as_str()),
                                bytes.as_slice(),
                            )
                            .store()?;
                    }

                    if let Some((publish_id, blob_id)) = resolve {
                        super::blobs::resolve_publish_intent_in_txn(&txn, &publish_id, blob_id)?;
                    }

                    Ok(())
                })();

                match result {
                    Ok(()) => {
                        txn.commit().store()?;
                        Ok(())
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;
    use crate::storage::Store;

    async fn repo() -> Repository {
        let store = Store::in_memory().expect("store");
        Repository::init(store).await.expect("init")
    }

    async fn seed_uploads(repo: &Repository, keys: &[&str], uploads_per_key: usize) {
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        for key in keys {
            for i in 0..uploads_per_key {
                repo.upsert_multipart_upload_state(MultipartUploadStateUpdate {
                    bucket: "b".into(),
                    key: (*key).to_string(),
                    upload_id: format!("u{i}"),
                    content_type: None,
                    user_metadata: serde_json::json!({}),
                    tags: serde_json::json!([]),
                    content: serde_json::json!({"part": i}),
                })
                .await
                .unwrap_or_else(|e| panic!("seed {key}/u{i}: {e:?}"));
            }
        }
    }

    fn ids(models: &[MultipartUploadRow]) -> Vec<(String, String)> {
        models
            .iter()
            .map(|m| (m.object_id.clone(), m.upload_id.clone()))
            .collect()
    }

    #[tokio::test]
    async fn list_multipart_uploads_bounds_rows_to_limit() {
        let repo = repo().await;
        seed_uploads(&repo, &["a", "b", "c", "d", "e"], 1).await;

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, None, None, Some(2))
            .await
            .expect("page 1");
        assert_eq!(
            ids(&models),
            [("a".into(), "u0".into()), ("b".into(), "u0".into())],
            "the page must hold no more than the limit"
        );
        assert!(is_truncated, "sentinel row must report truncation");

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, None, None, Some(5))
            .await
            .expect("full page");
        assert_eq!(models.len(), 5);
        assert!(!is_truncated);
    }

    #[tokio::test]
    async fn upload_page_two_starts_after_both_markers() {
        let repo = repo().await;
        seed_uploads(&repo, &["a", "b", "c", "d"], 2).await;

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, None, None, Some(3))
            .await
            .expect("page 1");
        assert_eq!(
            ids(&models),
            [
                ("a".into(), "u0".into()),
                ("a".into(), "u1".into()),
                ("b".into(), "u0".into()),
            ]
        );
        assert!(is_truncated);

        let last = models.last().expect("page 1").clone();
        let last_key = last.object_id.clone();
        let last_upload = last.upload_id.clone();

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, Some(&last_key), Some(&last_upload), Some(3))
            .await
            .expect("page 2");
        assert_eq!(
            ids(&models),
            [
                ("b".into(), "u1".into()),
                ("c".into(), "u0".into()),
                ("c".into(), "u1".into()),
            ],
            "continuation markers must resume at the next row, not repeat or skip"
        );
        assert!(is_truncated);

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, Some(&last_key), Some(&last_upload), None)
            .await
            .expect("tail");
        assert_eq!(
            ids(&models),
            [
                ("b".into(), "u1".into()),
                ("c".into(), "u0".into()),
                ("c".into(), "u1".into()),
                ("d".into(), "u0".into()),
                ("d".into(), "u1".into()),
            ]
        );
        assert!(!is_truncated, "last page must not report truncation");
    }

    #[tokio::test]
    async fn upload_key_marker_alone_skips_the_whole_marker_key() {
        let repo = repo().await;
        seed_uploads(&repo, &["a", "b"], 2).await;

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, Some("a"), None, Some(10))
            .await
            .expect("list");
        assert_eq!(
            ids(&models),
            [("b".into(), "u0".into()), ("b".into(), "u1".into())]
        );
        assert!(!is_truncated);
    }

    #[tokio::test]
    async fn negative_max_uploads_is_invalid_argument() {
        let repo = repo().await;
        seed_uploads(&repo, &["a"], 1).await;

        let err = repo
            .list_multipart_uploads("b", None, None, None, Some(-1))
            .await
            .expect_err("negative max-uploads must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn max_uploads_above_ceiling_is_clamped() {
        let repo = repo().await;
        seed_uploads(&repo, &["a", "b"], 1).await;

        let (models, is_truncated) = repo
            .list_multipart_uploads("b", None, None, None, Some(5000))
            .await
            .expect("oversized max-uploads must be clamped, not rejected");
        assert_eq!(models.len(), 2);
        assert!(!is_truncated);
    }

    #[tokio::test]
    async fn a_part_update_resolves_its_publish_intent_with_the_blob_id() {
        let repo = repo().await;
        seed_uploads(&repo, &["k"], 1).await;
        repo.record_publish_intent("pid".into(), "b".into(), "k".into(), 1)
            .await
            .expect("record intent");

        repo.cas_update_multipart_content_and_resolve("b", "k", "u0", "pid", "blob-1", |content| {
            *content = serde_json::json!({
                "1": {"hash": "d", "metadata_items": [{"id": "blob-1", "offset": 0, "size": 1}]}
            });
            Ok(())
        })
        .await
        .expect("update part");

        let state = repo
            .get_multipart_upload_state("b", "k", "u0")
            .await
            .expect("load state");
        assert_eq!(
            state.content["1"]["metadata_items"][0]["id"], "blob-1",
            "the part must link the new blob: {:?}",
            state.content
        );

        let intents = repo.store.all_publish_intents();
        let intent = intents
            .iter()
            .find(|row| row.publish_id == "pid")
            .expect("intent row");
        assert!(
            intent.resolved_at.is_some(),
            "the part update must resolve its intent: {intent:?}"
        );
        assert_eq!(intent.blob_id.as_deref(), Some("blob-1"));
    }

    #[tokio::test]
    async fn a_missing_publish_intent_rolls_back_the_part_update() {
        let repo = repo().await;
        seed_uploads(&repo, &["k"], 1).await;
        let before = repo
            .get_multipart_upload_state("b", "k", "u0")
            .await
            .expect("load state")
            .content;

        let err = repo
            .cas_update_multipart_content_and_resolve(
                "b",
                "k",
                "u0",
                "missing",
                "blob-1",
                |content| {
                    *content = serde_json::json!({
                        "1": {"hash": "d", "metadata_items": [{"id": "blob-1", "offset": 0, "size": 1}]}
                    });
                    Ok(())
                },
            )
            .await
            .expect_err("a missing intent must fail the part update");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        let after = repo
            .get_multipart_upload_state("b", "k", "u0")
            .await
            .expect("load state")
            .content;
        assert_eq!(after, before, "the part update must roll back");
    }

    #[tokio::test]
    async fn a_resolved_intent_is_left_for_the_reconciler() {
        let repo = repo().await;
        repo.record_publish_intent("pid".into(), "b".into(), "k".into(), 1)
            .await
            .expect("record intent");
        repo.resolve_publish_intent("pid", "blob-1".into())
            .await
            .expect("resolve");
        repo.store.set_publish_intent_created_at(
            "pid",
            chrono::Local::now().to_utc() - chrono::Duration::hours(2),
        );

        let pending = repo
            .pending_publish_intents(chrono::Local::now().to_utc(), 10)
            .await
            .expect("pending");
        assert!(pending.is_empty(), "a resolved intent is never swept");
        assert_eq!(
            repo.store.all_publish_intents().len(),
            1,
            "the resolved row is the only record of the key it produced"
        );
    }
}
