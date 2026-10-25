use std::collections::BTreeMap;

use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::{IntoCondition, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Set, TransactionTrait,
};
use tracing::instrument;

use super::entity;
use super::{ObjectWrite, PutCondition, Repository};
use crate::s3::objects::checked_max_uploads;
use crate::s3::types::MultipartUploadPart;

/// Field bundle for writing an in-progress multipart upload's state row.
///
/// Three of these are positional JSON payloads, so a swapped pair would
/// compile cleanly and silently corrupt the upload; naming the fields
/// keeps every call site self-checking.
#[derive(Debug)]
pub(crate) struct MultipartUploadStateUpdate {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) upload_id: String,
    pub(crate) content_type: Option<String>,
    /// x-amz-meta-* map as a JSON object; empty object when none.
    pub(crate) user_metadata: serde_json::Value,
    /// Object tag-set as a JSON array of {key, value}; empty array when none.
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

impl Repository {
    #[instrument(skip(self), level = "debug", err)]
    pub async fn upsert_multipart_upload_state(
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

        let active_model = entity::multipart_upload_state::ActiveModel {
            bucket_id: Set(bucket),
            object_id: Set(key),
            upload_id: Set(upload_id),
            content_type: Set(content_type),
            user_metadata: Set(user_metadata),
            tags: Set(Some(tags)),
            content: Set(content),
        };

        entity::multipart_upload_state::Entity::insert(active_model)
            .on_conflict(
                OnConflict::columns([
                    entity::multipart_upload_state::Column::BucketId,
                    entity::multipart_upload_state::Column::ObjectId,
                    entity::multipart_upload_state::Column::UploadId,
                ])
                .update_columns([
                    entity::multipart_upload_state::Column::ContentType,
                    entity::multipart_upload_state::Column::Tags,
                    entity::multipart_upload_state::Column::Content,
                ])
                .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    /// Build the bounded page query for [`Self::list_multipart_uploads`].
    ///
    /// `fetch_max` is the SQL `LIMIT` (the page size plus the truncation
    /// sentinel). Split out so the rendered SQL — `LIMIT` and marker
    /// predicate included — is assertable without a database.
    fn multipart_uploads_page_query(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        fetch_max: u64,
    ) -> sea_orm::Select<entity::multipart_upload_state::Entity> {
        entity::multipart_upload_state::Entity::find()
            .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket))
            // "Strictly after the cursor": rows past the marked key, plus the
            // marked key's uploads after the marked upload. An `upload_id_marker`
            // only narrows within the marked key; without a `key_marker` there
            // is nothing to narrow and the marker is ignored, as S3 specifies.
            .filter(match (key_marker, upload_id_marker) {
                (Some(km), Some(uid)) => Condition::any()
                    .add(entity::multipart_upload_state::Column::ObjectId.gt(km))
                    .add(
                        Condition::all()
                            .add(entity::multipart_upload_state::Column::ObjectId.eq(km))
                            .add(entity::multipart_upload_state::Column::UploadId.gt(uid)),
                    )
                    .into_condition(),
                (km, _) => km
                    .map(|km| {
                        entity::multipart_upload_state::Column::ObjectId
                            .gt(km)
                            .into_condition()
                    })
                    .unwrap_or_else(|| Condition::all().into_condition()),
            })
            .order_by_asc(entity::multipart_upload_state::Column::ObjectId)
            .order_by_asc(entity::multipart_upload_state::Column::UploadId)
            .limit(fetch_max)
            .apply_if(prefix, |query, p| {
                query.filter(entity::multipart_upload_state::Column::ObjectId.starts_with(p))
            })
    }

    /// List one page of the bucket's in-flight multipart uploads.
    ///
    /// Rows come back ordered `ObjectId ASC, UploadId ASC` and the page is
    /// bounded by a SQL `LIMIT`, so a paginated walk never materialises the
    /// whole table (with its `content` / `tags` / `user_metadata` JSON
    /// columns) per page.
    ///
    /// The `(key_marker, upload_id_marker)` boundary is a SQL predicate rather
    /// than a post-fetch filter: with a `key_marker` alone the cursor is the
    /// key itself, and with an `upload_id_marker` it is that exact row.
    ///
    /// `max_uploads` is validated (negatives rejected, values above the S3
    /// ceiling clamped) and the query fetches `max_uploads + 1` rows: the
    /// returned bool is the truncation sentinel (true means one more row
    /// existed beyond the page) and the returned vector holds at most
    /// `max_uploads` rows, so the caller derives `next_key_marker` /
    /// `next_upload_id_marker` from its last row.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        max_uploads: Option<i32>,
    ) -> S3Result<(Vec<entity::multipart_upload_state::Model>, bool)> {
        let max_uploads = checked_max_uploads(max_uploads)?;
        let limit =
            usize::try_from(max_uploads).map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
        // Fetch-limit + 1: the sentinel row is dropped before returning, so
        // `is_truncated` comes from real remaining data and not a second query.
        let fetch_max = limit.saturating_add(1);

        let query = Self::multipart_uploads_page_query(
            bucket,
            prefix,
            key_marker,
            upload_id_marker,
            fetch_max as u64,
        );

        let mut models = query.all(&self.db).await.map_err(S3Error::internal_error)?;

        let is_truncated = models.len() > limit;
        if is_truncated {
            models.truncate(limit);
        }

        Ok((models, is_truncated))
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_bucket_upload_count(&self, bucket: &str) -> S3Result<u64> {
        let count = entity::multipart_upload_state::Entity::find()
            .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket))
            .count(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(count)
    }

    pub async fn get_multipart_upload_state(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> S3Result<entity::multipart_upload_state::Model> {
        Self::find_multipart_upload_state(&self.db, bucket, key, upload_id).await
    }

    async fn find_multipart_upload_state<C: ConnectionTrait>(
        conn: &C,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> S3Result<entity::multipart_upload_state::Model> {
        let model = entity::multipart_upload_state::Entity::find_by_id((
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
        ))
        .one(conn)
        .await
        .map_err(S3Error::internal_error)?
        .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        Ok(model)
    }

    /// Delete one upload state without changing its blob references.
    ///
    /// Callers that must keep the state and its references together use
    /// [`Self::delete_multipart_upload_state_releasing_blobs`] instead.
    #[allow(dead_code)]
    #[instrument(skip(self), level = "debug", err)]
    pub async fn delete_multipart_upload_state(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> S3Result<Option<entity::multipart_upload_state::Model>> {
        let model = entity::multipart_upload_state::Entity::delete_by_id((
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
        ))
        .exec_with_returning(&self.db)
        .await
        .map_err(S3Error::internal_error)?;

        Ok(model)
    }

    /// Delete one upload state and drop its blob references in one
    /// transaction.
    ///
    /// `extract_ids` turns the stored content into the blob ids that the
    /// upload owns. If the state is absent, the result is `None` and nothing
    /// changes. If any step fails, the transaction rolls back, so the state
    /// and its references stay together for a retry.
    ///
    /// The returned ids are the blobs whose count reached zero. The caller
    /// must remove their backend bytes and rows.
    #[instrument(skip(self, extract_ids), level = "debug", err)]
    pub(crate) async fn delete_multipart_upload_state_releasing_blobs<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        extract_ids: F,
    ) -> S3Result<Option<Vec<String>>>
    where
        F: FnOnce(&serde_json::Value) -> S3Result<Vec<String>>,
    {
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result: S3Result<Option<Vec<String>>> = async {
            let model = entity::multipart_upload_state::Entity::find_by_id((
                bucket.to_string(),
                key.to_string(),
                upload_id.to_string(),
            ))
            .one(&txn)
            .await
            .map_err(S3Error::internal_error)?;

            let Some(model) = model else {
                return Ok(None);
            };

            let ids = extract_ids(&model.content)?;
            let released = Self::release_blob_refs_on(&txn, &ids).await?;

            entity::multipart_upload_state::Entity::delete_by_id((
                model.bucket_id,
                model.object_id,
                model.upload_id,
            ))
            .exec(&txn)
            .await
            .map_err(S3Error::internal_error)?;

            Ok(Some(released))
        }
        .await;

        match result {
            Ok(Some(released)) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(Some(released))
            }
            Ok(None) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Ok(None)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Publish a completed object and delete its upload state in one
    /// transaction.
    ///
    /// The caller builds the object from `expected_content`. The transaction
    /// checks that the stored content still equals it, so the new object
    /// owns every reference it names and no part added since the read is
    /// deleted with the state. A missing or changed state rolls the object
    /// write back.
    ///
    /// `release_ids` are the references the completion retires: the parts
    /// left out of the object and, for an unversioned bucket, the object it
    /// replaces. They are released in the same transaction, so a failure
    /// cannot publish the object while leaving an old reference behind.
    ///
    /// Returns the new version id and the released ids whose count reached
    /// zero. The caller must remove their backend bytes and rows.
    #[instrument(skip(self, completion), level = "debug", err)]
    pub(crate) async fn complete_multipart_upload(
        &self,
        completion: MultipartCompletion,
    ) -> S3Result<(String, Vec<String>)> {
        let MultipartCompletion {
            bucket,
            key,
            upload_id,
            expected_content,
            data,
            condition,
            release_ids,
        } = completion;

        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result: S3Result<(String, Vec<String>)> = async {
            let model = entity::multipart_upload_state::Entity::find_by_id((
                bucket.clone(),
                key.clone(),
                upload_id.clone(),
            ))
            .one(&txn)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

            if model.content != expected_content {
                return Err(S3Error::new(S3ErrorCode::InternalError));
            }

            let version_id = self
                .cas_put_object_txn(&txn, bucket, key, data, condition)
                .await?;

            let deleted = entity::multipart_upload_state::Entity::delete_by_id((
                model.bucket_id,
                model.object_id,
                model.upload_id,
            ))
            .exec(&txn)
            .await
            .map_err(S3Error::internal_error)?;

            if deleted.rows_affected != 1 {
                return Err(S3Error::new(S3ErrorCode::InternalError));
            }

            let released = Self::release_blob_refs_on(&txn, &release_ids).await?;

            Ok((version_id, released))
        }
        .await;

        match result {
            Ok((version_id, released)) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok((version_id, released))
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Store one part and release the references of the part it replaces in
    /// one transaction.
    ///
    /// Re-uploading a part number replaces the old metadata. Without this
    /// transaction the old blob would lose its holder but keep its reference,
    /// so a retried or repeated upload would leak one blob per attempt.
    ///
    /// A concurrent content change retries the compare-and-swap. The returned
    /// ids are the replaced part's blobs whose count reached zero; the caller
    /// must remove their backend bytes and rows.
    #[instrument(skip(self, part), level = "debug", err)]
    pub(crate) async fn set_multipart_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        part: MultipartUploadPart,
        resolve: Option<(&str, &str)>,
    ) -> S3Result<Vec<String>> {
        const MAX_ATTEMPTS: usize = 8;

        for _ in 0..MAX_ATTEMPTS {
            let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

            let result: S3Result<Option<Vec<String>>> = async {
                let model = entity::multipart_upload_state::Entity::find_by_id((
                    bucket.to_string(),
                    key.to_string(),
                    upload_id.to_string(),
                ))
                .one(&txn)
                .await
                .map_err(S3Error::internal_error)?
                .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

                let mut parts: BTreeMap<i32, MultipartUploadPart> =
                    serde_json::from_value(model.content.clone())
                        .map_err(S3Error::internal_error)?;

                let previous = parts.insert(part_number, part.clone());
                let new_content = serde_json::to_value(&parts).map_err(S3Error::internal_error)?;

                if new_content != model.content {
                    let update = entity::multipart_upload_state::Entity::update_many()
                        .col_expr(
                            entity::multipart_upload_state::Column::Content,
                            Expr::value(new_content),
                        )
                        .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket))
                        .filter(entity::multipart_upload_state::Column::ObjectId.eq(key))
                        .filter(entity::multipart_upload_state::Column::UploadId.eq(upload_id))
                        .filter(
                            entity::multipart_upload_state::Column::Content
                                .eq(model.content.clone()),
                        )
                        .exec(&txn)
                        .await
                        .map_err(S3Error::internal_error)?;

                    if update.rows_affected == 0 {
                        // Another writer changed the content; retry from its
                        // new value.
                        return Ok(None);
                    }
                }

                let released = match previous {
                    Some(previous) => {
                        let ids: Vec<String> = previous
                            .metadata_items
                            .into_iter()
                            .map(|item| item.id)
                            .collect();
                        Self::release_blob_refs_on(&txn, &ids).await?
                    }
                    None => Vec::new(),
                };

                // The part update and its publish-intent resolution commit
                // together. A resolve that ran after the commit could fail
                // and leave a pending row for a part that is already linked,
                // so the reclaim pass would then delete its bytes.
                if let Some((publish_id, blob_id)) = resolve {
                    Self::resolve_publish_intent_txn(&txn, publish_id, blob_id.to_string()).await?;
                }

                Ok(Some(released))
            }
            .await;

            match result {
                Ok(Some(released)) => {
                    txn.commit().await.map_err(S3Error::internal_error)?;
                    return Ok(released);
                }
                Ok(None) => {
                    txn.rollback().await.map_err(S3Error::internal_error)?;
                }
                Err(err) => {
                    txn.rollback().await.map_err(S3Error::internal_error)?;
                    return Err(err);
                }
            }
        }

        Err(S3Error::internal_error(std::io::Error::other(format!(
            "multipart upload state changed concurrently {} times",
            MAX_ATTEMPTS
        ))))
    }

    #[allow(dead_code)]
    #[instrument(skip(self, action), level = "debug", err)]
    pub async fn cas_update_multipart_content<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()>,
    {
        self.cas_update_multipart_content_inner(bucket, key, upload_id, None, action)
            .await
    }

    /// Commit a part update and resolve its publish intent in one transaction.
    ///
    /// The content update and the resolved intent must commit together. A
    /// resolve that ran after the commit could fail and leave a pending row
    /// for a part that is already linked, and the reclaim pass would then
    /// delete its bytes.
    #[allow(dead_code)]
    #[instrument(skip(self, action), level = "debug", err)]
    pub async fn cas_update_multipart_content_and_resolve<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        publish_id: &str,
        blob_id: &str,
        action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()>,
    {
        self.cas_update_multipart_content_inner(
            bucket,
            key,
            upload_id,
            Some((publish_id, blob_id)),
            action,
        )
        .await
    }

    async fn cas_update_multipart_content_inner<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        resolve: Option<(&str, &str)>,
        mut action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()>,
    {
        const MAX_ATTEMPTS: usize = 8;

        for _ in 0..MAX_ATTEMPTS {
            let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

            let result: S3Result<bool> = async {
                let model = Self::find_multipart_upload_state(&txn, bucket, key, upload_id).await?;

                let mut new_content = model.content.clone();
                action(&mut new_content)?;

                if new_content != model.content {
                    let result = entity::multipart_upload_state::Entity::update_many()
                        .col_expr(
                            entity::multipart_upload_state::Column::Content,
                            Expr::value(new_content),
                        )
                        .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket))
                        .filter(entity::multipart_upload_state::Column::ObjectId.eq(key))
                        .filter(entity::multipart_upload_state::Column::UploadId.eq(upload_id))
                        .filter(
                            entity::multipart_upload_state::Column::Content
                                .eq(model.content.clone()),
                        )
                        .exec(&txn)
                        .await
                        .map_err(S3Error::internal_error)?;

                    if result.rows_affected != 1 {
                        return Ok(false);
                    }
                }

                if let Some((publish_id, blob_id)) = resolve {
                    Self::resolve_publish_intent_txn(&txn, publish_id, blob_id.to_string()).await?;
                }

                Ok(true)
            }
            .await;

            match result {
                Ok(true) => {
                    txn.commit().await.map_err(S3Error::internal_error)?;
                    return Ok(());
                }
                Ok(false) => {
                    txn.rollback().await.map_err(S3Error::internal_error)?;
                }
                Err(err) => {
                    txn.rollback().await.map_err(S3Error::internal_error)?;
                    return Err(err);
                }
            }
        }

        Err(S3Error::internal_error(std::io::Error::other(format!(
            "multipart upload state changed concurrently {} times",
            MAX_ATTEMPTS
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn repo() -> Repository {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        Repository::init(db).await.expect("init")
    }

    /// Seed `keys`, each with `uploads_per_key` upload ids `u0..uN`.
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

    fn page_sql(
        key_marker: Option<&str>,
        upload_id_marker: Option<&str>,
        fetch_max: u64,
    ) -> String {
        Repository::multipart_uploads_page_query("b", None, key_marker, upload_id_marker, fetch_max)
            .build(sea_orm::DbBackend::Sqlite)
            .to_string()
    }

    fn ids(models: &[entity::multipart_upload_state::Model]) -> Vec<(String, String)> {
        models
            .iter()
            .map(|m| (m.object_id.clone(), m.upload_id.clone()))
            .collect()
    }

    #[test]
    fn upload_page_query_carries_sql_limit() {
        let sql = page_sql(None, None, 4);
        assert!(
            sql.contains("LIMIT 4"),
            "upload page query must bound rows in SQL: {sql}"
        );
    }

    #[test]
    fn upload_page_query_pushes_markers_into_sql() {
        let sql = page_sql(Some("k1"), Some("u1"), 4);
        assert!(sql.contains("LIMIT 4"), "missing limit: {sql}");
        assert!(
            sql.contains(r#""object_id" >"#),
            "key marker must be a SQL predicate: {sql}"
        );
        assert!(
            sql.contains(r#""upload_id" >"#),
            "upload-id marker must be a SQL predicate: {sql}"
        );

        // Without a key marker an upload-id marker narrows nothing.
        let sql = page_sql(None, Some("u1"), 4);
        assert!(
            !sql.contains(r#""upload_id" >"#),
            "upload-id marker alone must not filter: {sql}"
        );
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
            "SQL limit must cap the returned page"
        );
        assert!(is_truncated, "sentinel row must report truncation");

        // Last page exactly fills: no truncation, no sentinel.
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

        // 5000 is above the S3 ceiling but not a rejection: every seeded row
        // comes back, proving the query did not fail on the oversized value.
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

        let intent = entity::publish_intent::Entity::find_by_id("pid".to_string())
            .one(&repo.db)
            .await
            .expect("load intent")
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

        // A part that commits while its intent cannot resolve would leave the
        // reclaim pass a pending row for live bytes. The update must roll
        // back instead.
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
}
