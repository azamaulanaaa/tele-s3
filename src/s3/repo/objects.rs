use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ColumnTrait, Condition, EntityTrait, ExprTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    prelude::Expr,
    sea_query::{OnConflict, Query},
};
use tracing::instrument;

use super::entity;
use super::{ObjectWrite, PutCondition, Repository};

impl Repository {
    async fn get_latest_model(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<entity::object::Model>> {
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
        let version_id = self
            .put_object_internal(bucket, key, data, versioning, now)
            .await?;
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
                size: Set(Self::checked_size(data.size)?),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(data.tags),
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
                        entity::object::Column::Tags,
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
                size: Set(Self::checked_size(data.size)?),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(data.tags),
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
                size: Set(Self::checked_size(data.size)?),
                last_modified: Set(now),
                content_type: Set(data.content_type),
                etag: Set(data.etag),
                user_metadata: Set(data.user_metadata),
                tags: Set(data.tags),
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
                        entity::object::Column::Tags,
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
        let result = query
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;
        if result.rows_affected == 0 {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey));
        }
        Ok(())
    }

    /// Version id of the current latest entry for a key, if any.
    pub async fn latest_version_id(&self, bucket: &str, key: &str) -> S3Result<Option<String>> {
        Ok(self
            .get_latest_model(bucket, key)
            .await?
            .map(|m| m.version_id))
    }

    /// Latest version row even if it is a delete marker (`None` when the
    /// key has no versions at all). Used to attach delete-marker headers
    /// to GET/HEAD errors.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_latest_raw(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<entity::object::Model>> {
        self.get_latest_model(bucket, key).await
    }

    #[instrument(skip(self, data), level = "debug", err)]
    pub async fn upsert_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
    ) -> S3Result<String> {
        let bucket_model = self.get_bucket(&bucket).await?;
        let versioning = bucket_model.versioning_status;
        let now = chrono::Local::now().to_utc();
        self.put_object_internal(bucket, key, data, versioning, now)
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn object_exists(&self, bucket: &str, key: &str) -> S3Result<bool> {
        let m = self.get_latest_model(bucket, key).await?;
        Ok(m.is_some_and(|v| !v.is_delete_marker))
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<entity::object::Model> {
        let m = self
            .get_latest_model(bucket, key)
            .await?
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
                    .ok_or_else(|| {
                        S3Error::internal_error(std::io::Error::other(
                            "delete marker vanished immediately after insert",
                        ))
                    })?;

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
}
