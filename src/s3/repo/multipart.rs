use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::{IntoCondition, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
    QueryTrait, Set,
};
use tracing::instrument;

use super::Repository;
use super::entity;
use crate::s3::objects::checked_max_uploads;

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
        let model = entity::multipart_upload_state::Entity::find_by_id((
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
        ))
        .one(&self.db)
        .await
        .map_err(S3Error::internal_error)?
        .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        Ok(model)
    }

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

    #[instrument(skip(self, action), level = "debug", err)]
    pub async fn cas_update_multipart_content<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut action: F,
    ) -> S3Result<()>
    where
        F: FnMut(&mut serde_json::Value) -> S3Result<()>,
    {
        const MAX_ATTEMPTS: usize = 8;

        for _ in 0..MAX_ATTEMPTS {
            let model = self
                .get_multipart_upload_state(bucket, key, upload_id)
                .await?;

            let mut new_content = model.content.clone();
            action(&mut new_content)?;

            if new_content == model.content {
                return Ok(());
            }

            let result = entity::multipart_upload_state::Entity::update_many()
                .col_expr(
                    entity::multipart_upload_state::Column::Content,
                    Expr::value(new_content),
                )
                .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket))
                .filter(entity::multipart_upload_state::Column::ObjectId.eq(key))
                .filter(entity::multipart_upload_state::Column::UploadId.eq(upload_id))
                .filter(entity::multipart_upload_state::Column::Content.eq(model.content.clone()))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            if result.rows_affected == 1 {
                return Ok(());
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
}
