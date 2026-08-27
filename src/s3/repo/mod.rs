use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait,
    ExprTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, SqlErr,
    prelude::Expr,
    sea_query::{OnConflict, Query},
};
use tracing::instrument;

pub mod entity;

/// Precondition for a compare-and-swap object write.
pub enum PutCondition {
    /// Unconditional upsert (no If-Match / If-None-Match header).
    None,
    /// If-Match with a specific ETag: write only if the object exists
    /// and its stored ETag matches. Missing object → NoSuchKey.
    IfMatch(String),
    /// If-Match: * — write only if the object exists.
    IfMatchAny,
    /// If-None-Match: * — create-only; any existing object → 412.
    IfNoneMatchAny,
    /// If-None-Match with a specific ETag: fail only if the object exists
    /// and its stored ETag matches.
    IfNoneMatch(String),
}

pub struct Repository {
    pub db: DatabaseConnection,
}

/// Field bundle for writing an object row.
pub struct ObjectWrite {
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    /// Ordered blob-item list describing the object's content.
    pub content: serde_json::Value,
    /// x-amz-meta-* map as a JSON object; empty object when none.
    pub user_metadata: serde_json::Value,
    /// Client-provided checksums as a JSON object keyed by algorithm;
    /// empty object when none.
    pub checksums: serde_json::Value,
}

impl Repository {
    #[instrument(skip(db), level = "debug")]
    pub async fn init(db: DatabaseConnection) -> anyhow::Result<Self> {
        Self::sync_table(&db).await?;

        Ok(Self { db })
    }

    #[instrument(skip(db), level = "debug", err)]
    async fn sync_table(db: &DatabaseConnection) -> Result<(), DbErr> {
        db.get_schema_registry(concat!(module_path!(), "::entity"))
            .sync(db)
            .await?;

        Ok(())
    }

    // ---- Bucket versioning helpers ----

    pub async fn get_bucket_versioning(&self, bucket: &str) -> S3Result<Option<String>> {
        let model = self.get_bucket(bucket).await?;
        Ok(model.versioning_status)
    }

    pub async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: Option<String>,
    ) -> S3Result<()> {
        // Ensure bucket exists
        self.get_bucket(bucket).await?;

        // status is Option: Some("Enabled") / Some("Suspended") / None (reset to disabled? but API only sends Enabled/Suspended)
        // We treat None as disabled (should not happen via Put, but allow)
        let res = entity::bucket::Entity::update_many()
            .col_expr(
                entity::bucket::Column::VersioningStatus,
                Expr::value(status.clone()),
            )
            .filter(entity::bucket::Column::Id.eq(bucket))
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        if res.rows_affected == 0 {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }
        Ok(())
    }

    // ---- Bucket ops ----

    #[instrument(skip(self), level = "debug", err)]
    pub async fn create_bucket(&self, name: String, region: Option<String>) -> S3Result<()> {
        let active_model = entity::bucket::ActiveModel {
            id: Set(name),
            region: Set(region),
            created_at: Set(chrono::Local::now().to_utc()),
            versioning_status: Set(None),
        };

        entity::bucket::Entity::insert(active_model)
            .exec(&self.db)
            .await
            .map_err(|e| match e {
                DbErr::Exec(e) => {
                    let err = DbErr::Exec(e);

                    match err.sql_err() {
                        Some(SqlErr::UniqueConstraintViolation(_)) => {
                            S3Error::new(S3ErrorCode::BucketAlreadyExists)
                        }
                        _ => S3Error::internal_error(err),
                    }
                }

                e => S3Error::internal_error(e),
            })?;

        Ok(())
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn list_buckets(&self) -> S3Result<Vec<entity::bucket::Model>> {
        let buckets = entity::bucket::Entity::find()
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(buckets)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        entity::bucket::Entity::delete_by_id(name)
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_bucket_object_count(&self, name: &str) -> S3Result<u64> {
        // Count all versions including delete markers; bucket is non-empty if any version exists.
        let object_count = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(name))
            .count(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(object_count)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn bucket_exists(&self, name: &str) -> S3Result<bool> {
        let bucket_exists = entity::bucket::Entity::find_by_id(name)
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .is_some();

        Ok(bucket_exists)
    }

    // ---- Object version helpers ----

    async fn get_latest_model(&self, bucket: &str, key: &str) -> S3Result<Option<entity::object::Model>> {
        let m = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.eq(key))
            .filter(entity::object::Column::IsLatest.eq(true))
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?;
        Ok(m)
    }

    pub async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> S3Result<entity::object::Model> {
        let m = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.eq(key))
            .filter(entity::object::Column::VersionId.eq(version_id))
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;
        Ok(m)
    }

    // ---- Compare-and-swap object write with versioning ----

    #[instrument(skip(self, data, condition), level = "debug", err)]
    pub async fn cas_put_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        // Check bucket versioning
        let bucket_model = self.get_bucket(&bucket).await?;
        let versioning = bucket_model.versioning_status.clone();

        let now = chrono::Local::now().to_utc();

        // Helper to evaluate condition against latest
        let latest_opt = self.get_latest_model(&bucket, &key).await?;
        // Existence for condition means latest exists and is not delete marker
        let exists_not_deleted = latest_opt.as_ref().is_some_and(|m| !m.is_delete_marker);
        let current_etag = if exists_not_deleted {
            latest_opt.as_ref().and_then(|m| m.etag.clone())
        } else {
            None
        };

        // Evaluate condition before write
        match &condition {
            PutCondition::IfMatch(expected) => {
                if !exists_not_deleted {
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                if current_etag.as_deref() != Some(expected.as_str()) {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
            PutCondition::IfMatchAny => {
                if !exists_not_deleted {
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
            }
            PutCondition::IfNoneMatchAny => {
                if exists_not_deleted {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
            PutCondition::IfNoneMatch(expected) => {
                if exists_not_deleted && current_etag.as_deref() == Some(expected.as_str()) {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
            PutCondition::None => {}
        }

        // Now perform write
        let version_id = self.put_object_internal(bucket, key, data, versioning, now).await?;
        Ok(version_id)
    }

    async fn put_object_internal(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
        versioning: Option<String>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> S3Result<String> {
        if versioning.is_none() {
            // Non-versioned bucket: single null version via upsert
            let version_id = "null".to_string();
            let active_model = entity::object::ActiveModel {
                bucket_id: Set(bucket.clone()),
                id: Set(key.clone()),
                version_id: Set(version_id.clone()),
                is_latest: Set(true),
                is_delete_marker: Set(false),
                size: Set(data.size as u32),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(serde_json::json!([])),
                checksums: Set(data.checksums),
                content: Set(data.content),
            };

            entity::object::Entity::insert(active_model)
                .on_conflict(
                    OnConflict::columns([
                        entity::object::Column::BucketId,
                        entity::object::Column::Id,
                        entity::object::Column::VersionId,
                    ])
                    .update_columns([
                        entity::object::Column::Size,
                        entity::object::Column::LastModified,
                        entity::object::Column::ContentType,
                        entity::object::Column::Etag,
                        entity::object::Column::Content,
                        entity::object::Column::UserMetadata,
                        entity::object::Column::Checksums,
                        entity::object::Column::IsLatest,
                        entity::object::Column::IsDeleteMarker,
                    ])
                    .to_owned(),
                )
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            Ok(version_id)
        } else if versioning.as_deref() == Some("Enabled") {
            // Versioned enabled: create new version with uuid
            let version_id = uuid::Uuid::new_v4().to_string();

            // Demote old latest
            entity::object::Entity::update_many()
                .col_expr(entity::object::Column::IsLatest, Expr::value(false))
                .filter(entity::object::Column::BucketId.eq(bucket.clone()))
                .filter(entity::object::Column::Id.eq(key.clone()))
                .filter(entity::object::Column::IsLatest.eq(true))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let active_model = entity::object::ActiveModel {
                bucket_id: Set(bucket),
                id: Set(key),
                version_id: Set(version_id.clone()),
                is_latest: Set(true),
                is_delete_marker: Set(false),
                size: Set(data.size as u32),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(serde_json::json!([])),
                checksums: Set(data.checksums),
                content: Set(data.content),
            };

            entity::object::Entity::insert(active_model)
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            Ok(version_id)
        } else {
            // Suspended: use null version id, demote old and upsert null
            // Demote old latest (including null if it was latest, we will re-enable it)
            entity::object::Entity::update_many()
                .col_expr(entity::object::Column::IsLatest, Expr::value(false))
                .filter(entity::object::Column::BucketId.eq(bucket.clone()))
                .filter(entity::object::Column::Id.eq(key.clone()))
                .filter(entity::object::Column::IsLatest.eq(true))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let version_id = "null".to_string();
            let active_model = entity::object::ActiveModel {
                bucket_id: Set(bucket.clone()),
                id: Set(key.clone()),
                version_id: Set(version_id.clone()),
                is_latest: Set(true),
                is_delete_marker: Set(false),
                size: Set(data.size as u32),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(serde_json::json!([])),
                checksums: Set(data.checksums),
                content: Set(data.content),
            };

            entity::object::Entity::insert(active_model)
                .on_conflict(
                    OnConflict::columns([
                        entity::object::Column::BucketId,
                        entity::object::Column::Id,
                        entity::object::Column::VersionId,
                    ])
                    .update_columns([
                        entity::object::Column::Size,
                        entity::object::Column::LastModified,
                        entity::object::Column::ContentType,
                        entity::object::Column::Etag,
                        entity::object::Column::Content,
                        entity::object::Column::UserMetadata,
                        entity::object::Column::Checksums,
                        entity::object::Column::IsLatest,
                        entity::object::Column::IsDeleteMarker,
                    ])
                    .to_owned(),
                )
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            Ok(version_id)
        }
    }

    /// Register a freshly created backend blob with a single reference.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn register_new_blob(&self, id: String, size: u64) -> S3Result<()> {
        let active_model = entity::blob::ActiveModel {
            id: Set(id),
            size: Set(size as u32),
            refs: Set(1),
            created_at: Set(chrono::Local::now().to_utc()),
        };

        entity::blob::Entity::insert(active_model)
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    /// Acquire one additional reference for each listed blob.
    #[instrument(skip(self, items), level = "debug", err)]
    pub async fn acquire_blob_refs(&self, items: &[(String, u64)]) -> S3Result<()> {
        for (id, size) in items {
            let existing = entity::blob::Entity::find_by_id(id)
                .one(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            if existing.is_some() {
                entity::blob::Entity::update_many()
                    .col_expr(
                        entity::blob::Column::Refs,
                        Expr::col(entity::blob::Column::Refs).add(1),
                    )
                    .filter(entity::blob::Column::Id.eq(id.clone()))
                    .exec(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;
            } else {
                let active_model = entity::blob::ActiveModel {
                    id: Set(id.clone()),
                    size: Set(*size as u32),
                    refs: Set(2),
                    created_at: Set(chrono::Local::now().to_utc()),
                };

                entity::blob::Entity::insert(active_model)
                    .on_conflict(
                        OnConflict::columns([entity::blob::Column::Id])
                            .do_nothing()
                            .to_owned(),
                    )
                    .exec(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;
            }
        }

        Ok(())
    }

    /// Drop one reference for each listed blob id.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn release_blob_refs(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        entity::blob::Entity::update_many()
            .col_expr(
                entity::blob::Column::Refs,
                Expr::col(entity::blob::Column::Refs).sub(1),
            )
            .filter(entity::blob::Column::Id.is_in(ids.to_vec()))
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        let released = entity::blob::Entity::find()
            .filter(entity::blob::Column::Refs.lte(0))
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        if released.is_empty() {
            return Ok(Vec::new());
        }

        entity::blob::Entity::delete_many()
            .filter(entity::blob::Column::Refs.lte(0))
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(released.into_iter().map(|model| model.id).collect())
    }

    pub async fn set_object_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: serde_json::Value,
    ) -> S3Result<()> {
        let mut query = entity::object::Entity::update_many()
            .col_expr(entity::object::Column::Tags, Expr::value(tags))
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.eq(key));
        if let Some(vid) = version_id {
            query = query.filter(entity::object::Column::VersionId.eq(vid));
        } else {
            query = query.filter(entity::object::Column::IsLatest.eq(true));
        }
        let result = query.exec(&self.db).await.map_err(S3Error::internal_error)?;
        if result.rows_affected == 0 {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey));
        }
        Ok(())
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_bucket(&self, name: &str) -> S3Result<entity::bucket::Model> {
        let bucket = entity::bucket::Entity::find_by_id(name)
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?;

        Ok(bucket)
    }

    #[instrument(skip(self, data), level = "debug", err)]
    pub async fn upsert_object(&self, bucket: String, key: String, data: ObjectWrite) -> S3Result<String> {
        let bucket_model = self.get_bucket(&bucket).await?;
        let versioning = bucket_model.versioning_status;
        let now = chrono::Local::now().to_utc();
        self.put_object_internal(bucket, key, data, versioning, now).await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn object_exists(&self, bucket: &str, key: &str) -> S3Result<bool> {
        let m = self.get_latest_model(bucket, key).await?;
        Ok(m.is_some_and(|v| !v.is_delete_marker))
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<entity::object::Model> {
        let m = self.get_latest_model(bucket, key).await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;
        if m.is_delete_marker {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey));
        }
        Ok(m)
    }

    /// Versioned delete: if version_id is Some, permanently delete that version.
    /// If None, create delete marker (if bucket versioning enabled) or permanently delete null version.
    /// Returns (deleted_model, is_delete_marker_created)
    pub async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<(Option<entity::object::Model>, bool)> {
        if let Some(vid) = version_id {
            // Permanent delete of specific version
            let model_opt = entity::object::Entity::find()
                .filter(entity::object::Column::BucketId.eq(bucket))
                .filter(entity::object::Column::Id.eq(key))
                .filter(entity::object::Column::VersionId.eq(vid))
                .one(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            let model = match model_opt {
                Some(m) => m,
                None => return Err(S3Error::new(S3ErrorCode::NoSuchKey)),
            };

            let was_latest = model.is_latest;

            // Delete that version row
            entity::object::Entity::delete_many()
                .filter(entity::object::Column::BucketId.eq(bucket))
                .filter(entity::object::Column::Id.eq(key))
                .filter(entity::object::Column::VersionId.eq(vid))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            // If it was latest, promote next latest (most recent)
            if was_latest {
                // Find most recent remaining version
                let next = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .order_by_desc(entity::object::Column::LastModified)
                    .one(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;
                if let Some(next_model) = next {
                    entity::object::Entity::update_many()
                        .col_expr(entity::object::Column::IsLatest, Expr::value(true))
                        .filter(entity::object::Column::BucketId.eq(bucket))
                        .filter(entity::object::Column::Id.eq(key))
                        .filter(entity::object::Column::VersionId.eq(next_model.version_id.clone()))
                        .exec(&self.db)
                        .await
                        .map_err(S3Error::internal_error)?;
                }
            }

            Ok((Some(model), false))
        } else {
            // No version_id: check bucket versioning status
            let bucket_model = self.get_bucket(bucket).await?;
            let status = bucket_model.versioning_status;
            if status.is_none() {
                // Non-versioned: permanent delete null
                let m = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .filter(entity::object::Column::VersionId.eq("null"))
                    .one(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;
                if let Some(model) = m.clone() {
                    entity::object::Entity::delete_many()
                        .filter(entity::object::Column::BucketId.eq(bucket))
                        .filter(entity::object::Column::Id.eq(key))
                        .filter(entity::object::Column::VersionId.eq("null"))
                        .exec(&self.db)
                        .await
                        .map_err(S3Error::internal_error)?;
                    Ok((Some(model), false))
                } else {
                    // No such key; S3 delete is idempotent, return Ok with no model
                    Ok((None, false))
                }
            } else {
                // Versioned: create delete marker
                let version_id = uuid::Uuid::new_v4().to_string();
                let now = chrono::Local::now().to_utc();

                // Demote old latest
                entity::object::Entity::update_many()
                    .col_expr(entity::object::Column::IsLatest, Expr::value(false))
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .filter(entity::object::Column::IsLatest.eq(true))
                    .exec(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;

                let active = entity::object::ActiveModel {
                    bucket_id: Set(bucket.to_string()),
                    id: Set(key.to_string()),
                    version_id: Set(version_id.clone()),
                    is_latest: Set(true),
                    is_delete_marker: Set(true),
                    size: Set(0),
                    last_modified: Set(now),
                    content_type: Set(None),
                    etag: Set(None),
                    user_metadata: Set(serde_json::json!({})),
                    tags: Set(serde_json::json!([])),
                    checksums: Set(serde_json::json!({})),
                    content: Set(serde_json::json!({"item": []})),
                };
                entity::object::Entity::insert(active)
                    .exec(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?;

                // Return marker info: caller can create response with version_id
                // We need to fetch the marker to return? We'll synthesize.
                let marker_model = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .filter(entity::object::Column::VersionId.eq(version_id.clone()))
                    .one(&self.db)
                    .await
                    .map_err(S3Error::internal_error)?
                    .unwrap();

                Ok((Some(marker_model), true))
            }
        }
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        marker: Option<String>,
        limit: u64,
    ) -> S3Result<Vec<entity::object::Model>> {
        // Only list latest, non-delete-marker versions
        let mut query = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::IsLatest.eq(true))
            .filter(entity::object::Column::IsDeleteMarker.eq(false))
            .filter(
                Condition::all()
                    .add_option(
                        marker
                            .clone()
                            .map(|marker| entity::object::Column::Id.gt(marker)),
                    )
                    .add_option(
                        prefix
                            .clone()
                            .map(|prefix| entity::object::Column::Id.starts_with(prefix)),
                    ),
            )
            .order_by_asc(entity::object::Column::Id);

        if let Some(delimiter) = delimiter {
            let prefix_len = prefix.clone().map(|v| v.len()).unwrap_or_default() as u32;

            query =
                query.filter(
                    Condition::any()
                        .add(
                            Expr::cust_with_exprs(
                                "INSTR(SUBSTR(?, ?), ?)",
                                [
                                    entity::object::Column::Id.into_expr(),
                                    (prefix_len + 1).into(),
                                    delimiter.clone().into(),
                                ],
                            )
                            .eq(0),
                        )
                        .add(
                            entity::object::Column::Id.in_subquery(
                                Query::select()
                                    .expr(entity::object::Column::Id.min())
                                    .from(entity::object::Entity)
                                    .distinct()
                                    .cond_where(
                                        Condition::all()
                                            .add(entity::object::Column::BucketId.eq(bucket))
                                            .add(entity::object::Column::IsLatest.eq(true))
                                            .add(entity::object::Column::IsDeleteMarker.eq(false))
                                            .add_option(marker.map(|marker| {
                                                entity::object::Column::Id.gt(marker)
                                            }))
                                            .add_option(prefix.clone().map(|prefix| {
                                                entity::object::Column::Id.starts_with(prefix)
                                            }))
                                            .add(
                                                Expr::cust_with_exprs(
                                                    "INSTR(SUBSTR(?, ?), ?)",
                                                    [
                                                        entity::object::Column::Id.into_expr(),
                                                        (prefix_len + 1).into(),
                                                        delimiter.clone().into(),
                                                    ],
                                                )
                                                .ne(0),
                                            ),
                                    )
                                    .add_group_by([Expr::cust_with_exprs(
                                        "SUBSTR(?, 1, ? + INSTR(SUBSTR(?, ?), ?))",
                                        [
                                            entity::object::Column::Id.into_expr(),
                                            prefix_len.into(),
                                            entity::object::Column::Id.into_expr(),
                                            (prefix_len + 1).into(),
                                            delimiter.clone().into(),
                                        ],
                                    )])
                                    .to_owned(),
                            ),
                        ),
                );
        }

        let models = query
            .limit(Some(limit))
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(models)
    }

    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<String>,
        _delimiter: Option<String>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
        max_keys: Option<i32>,
    ) -> S3Result<(Vec<entity::object::Model>, Vec<entity::object::Model>)> {
        // Returns (versions, delete_markers) split
        // Fetch all versions for bucket, ordered by key asc, last_modified desc
        let mut query = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .order_by_asc(entity::object::Column::Id)
            .order_by_desc(entity::object::Column::LastModified);

        if let Some(prefix) = prefix.clone() {
            query = query.filter(entity::object::Column::Id.starts_with(prefix));
        }

        let mut all = query.all(&self.db).await.map_err(S3Error::internal_error)?;

        // Handle delimiter: if specified, we need to return common prefixes? That is handled in S3 layer, not here.
        // We just return all versions; S3 layer will compute common prefixes.
        // For delimiter case, S3 spec groups keys. Our repo returns all versions, handler will fold.
        // But we need to apply key_marker/version_id_marker pagination:
        if let Some(km) = key_marker {
            if let Some(vid) = version_id_marker {
                // Find position of (km, vid)
                if let Some(pos) = all.iter().position(|m| m.id == km && m.version_id == vid) {
                    all = all[pos + 1..].to_vec();
                } else if let Some(pos) = all.iter().position(|m| m.id > km) {
                    // If exact vid not found, start after key_marker
                    all = all[pos..].to_vec();
                } else {
                    all.retain(|m| m.id > km);
                }
            } else {
                all.retain(|m| m.id > km);
            }
        }

        // Apply max_keys truncation in handler, but we can pre-truncate
        let limit = max_keys.unwrap_or(1000) as usize;
        let truncated = all.len() > limit;
        if truncated {
            all.truncate(limit);
        }

        // Split into versions vs delete markers
        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        for m in all {
            if m.is_delete_marker {
                delete_markers.push(m);
            } else {
                versions.push(m);
            }
        }

        // If delimiter provided, we need to filter versions to mimic list_objects delimiter behavior? 
        // For list_object_versions, delimiter grouping is similar but versions are still listed; common prefixes are separate.
        // Our handler will compute common prefixes from versions list, so we just return.

        Ok((versions, delete_markers))
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn upsert_multipart_upload_state(
        &self,
        bucket: String,
        key: String,
        upload_id: String,
        content_type: Option<String>,
        user_metadata: serde_json::Value,
        content: serde_json::Value,
    ) -> S3Result<()> {
        let active_model = entity::multipart_upload_state::ActiveModel {
            bucket_id: Set(bucket),
            object_id: Set(key),
            upload_id: Set(upload_id),
            content_type: Set(content_type),
            user_metadata: Set(user_metadata),
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
                    entity::multipart_upload_state::Column::Content,
                ])
                .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> S3Result<Vec<entity::multipart_upload_state::Model>> {
        let mut query = entity::multipart_upload_state::Entity::find()
            .filter(entity::multipart_upload_state::Column::BucketId.eq(bucket));

        if let Some(prefix) = prefix {
            query = query
                .filter(entity::multipart_upload_state::Column::ObjectId.starts_with(prefix))
                .order_by_asc(entity::multipart_upload_state::Column::ObjectId);
        }

        let models = query
            .order_by_asc(entity::multipart_upload_state::Column::ObjectId)
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(models)
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

    #[tokio::test]
    async fn cas_put_if_none_match_any_rejects_second_create() {
        let repo = repo().await;

        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let first = repo
            .cas_put_object(
                "b".into(),
                "k".into(),
                ObjectWrite {
                    size: 1,
                    content_type: None,
                    etag: Some("e1".into()),
                    content: serde_json::json!({}),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                },
                PutCondition::IfNoneMatchAny,
            )
            .await;
        assert!(
            first.is_ok(),
            "first create-only put should succeed: {first:?}"
        );

        let second = repo
            .cas_put_object(
                "b".into(),
                "k".into(),
                ObjectWrite {
                    size: 1,
                    content_type: None,
                    etag: Some("e2".into()),
                    content: serde_json::json!({}),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                },
                PutCondition::IfNoneMatchAny,
            )
            .await;

        match second {
            Ok(_) => panic!("second create-only put should fail"),
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(
                    msg.contains("PreconditionFailed"),
                    "expected PreconditionFailed, got: {msg}"
                );
            }
        }
    }
}
