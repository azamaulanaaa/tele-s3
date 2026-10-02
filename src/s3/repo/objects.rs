use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ColumnTrait, Condition, DatabaseTransaction, EntityTrait, ExprTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, Set, TransactionTrait,
    prelude::Expr,
    sea_query::{IntoCondition, OnConflict, Query},
};
use tracing::instrument;

use super::entity;
use super::{ObjectWrite, PutCondition, Repository};
use crate::s3::objects::MAX_KEYS_CEILING;

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

    /// Atomic compare-and-swap object write.
    ///
    /// The condition check and the demote+insert run inside one database
    /// transaction: concurrent writers serialize on the write lock instead
    /// of interleaving into zero (or two) `is_latest` rows, and a crash
    /// between demote and insert can no longer orphan the key.
    #[instrument(skip(self, data, condition), level = "debug", err)]
    pub async fn cas_put_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result = self
            .cas_put_object_txn(&txn, bucket, key, data, condition)
            .await;

        match result {
            Ok(version_id) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(version_id)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    /// Atomic publish: record the freshly written blob row *and* the object
    /// row in one transaction.
    ///
    /// `PutObject` used to run [`Self::cas_put_object`] as a second
    /// transaction after `register_new_blob`. The backend blob already
    /// existed at that point, so a crash (or any error) between the two left
    /// a `s3_blob` row with `refs = 1` that nothing references: invisible to
    /// every listing, never reclaimed. Composing the blob insert with the
    /// already-factored `cas_put_object_txn` closes the window — both rows
    /// appear together or not at all.
    ///
    /// Error mapping is unchanged: the condition check, the versioning
    /// branches and their codes all come from `cas_put_object_txn`.
    #[instrument(skip(self, data, condition), level = "debug", err)]
    pub async fn publish_object_with_new_blob(
        &self,
        blob_id: String,
        blob_size: u64,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result = async {
            Self::insert_blob(&txn, blob_id, blob_size).await?;
            self.cas_put_object_txn(&txn, bucket, key, data, condition)
                .await
        }
        .await;

        match result {
            Ok(version_id) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(version_id)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    async fn cas_put_object_txn(
        &self,
        txn: &DatabaseTransaction,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        // Check bucket versioning
        let bucket_model = Self::get_bucket_txn(txn, &bucket).await?;
        let versioning = bucket_model.versioning_status.clone();

        let now = chrono::Local::now().to_utc();

        // Helper to evaluate condition against latest
        let latest_opt = Self::get_latest_model_txn(txn, &bucket, &key).await?;
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
            .put_object_internal(txn, bucket, key, data, versioning, now)
            .await?;
        Ok(version_id)
    }

    async fn get_bucket_txn(
        txn: &DatabaseTransaction,
        name: &str,
    ) -> S3Result<entity::bucket::Model> {
        let bucket = entity::bucket::Entity::find_by_id(name)
            .one(txn)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?;

        Ok(bucket)
    }

    async fn get_latest_model_txn(
        txn: &DatabaseTransaction,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<entity::object::Model>> {
        let m = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.eq(key))
            .filter(entity::object::Column::IsLatest.eq(true))
            .one(txn)
            .await
            .map_err(S3Error::internal_error)?;
        Ok(m)
    }

    async fn put_object_internal(
        &self,
        txn: &DatabaseTransaction,
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
                .exec(txn)
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
                .exec(txn)
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
                .exec(txn)
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
                .exec(txn)
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
                .exec(txn)
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
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let bucket_model = Self::get_bucket_txn(&txn, &bucket).await?;
        let versioning = bucket_model.versioning_status;
        let now = chrono::Local::now().to_utc();
        let result = self
            .put_object_internal(&txn, bucket, key, data, versioning, now)
            .await;

        match result {
            Ok(version_id) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(version_id)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
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
    ///
    /// Every statement runs inside one transaction, mirroring
    /// [`Self::cas_put_object`]. That matters because "at most one row per key
    /// has `is_latest = true`" is enforced by the *transaction*, never by the
    /// schema: the `s3_object` primary key is `(bucket_id, id, version_id)` and
    /// there is no unique index on `is_latest` anywhere -- sea-orm's
    /// `schema-sync` derive cannot express a *partial* unique index
    /// (`WHERE is_latest = true`), and a plain unique index on `(bucket_id, id)`
    /// would be flat-out wrong, since every non-latest version of a key shares
    /// those two columns. The invariant is instead upheld by holding the write
    /// lock across the whole demote / insert / promote sequence, so a concurrent
    /// transaction on the same key serializes behind this one and never
    /// observes -- or leaves behind -- an intermediate state. Running these
    /// statements as separate implicit transactions would let a crash between
    /// demote and promote strand the key with zero `is_latest` rows (still
    /// listed by `ListObjectVersions`, invisible to `GetObject` forever), and
    /// would let a transacted `cas_put_object` interleaving at statement
    /// granularity leave two `is_latest` rows for `get_latest_model` to resolve
    /// arbitrarily.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<(Option<entity::object::Model>, bool)> {
        let txn = self.db.begin().await.map_err(S3Error::internal_error)?;

        let result = self
            .delete_object_versioned_txn(&txn, bucket, key, version_id)
            .await;

        match result {
            Ok(out) => {
                txn.commit().await.map_err(S3Error::internal_error)?;
                Ok(out)
            }
            Err(err) => {
                txn.rollback().await.map_err(S3Error::internal_error)?;
                Err(err)
            }
        }
    }

    async fn delete_object_versioned_txn(
        &self,
        txn: &DatabaseTransaction,
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
                .one(txn)
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
                .exec(txn)
                .await
                .map_err(S3Error::internal_error)?;

            // If it was latest, promote next latest (most recent)
            if was_latest {
                // Find most recent remaining version
                let next = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .order_by_desc(entity::object::Column::LastModified)
                    .one(txn)
                    .await
                    .map_err(S3Error::internal_error)?;
                if let Some(next_model) = next {
                    entity::object::Entity::update_many()
                        .col_expr(entity::object::Column::IsLatest, Expr::value(true))
                        .filter(entity::object::Column::BucketId.eq(bucket))
                        .filter(entity::object::Column::Id.eq(key))
                        .filter(entity::object::Column::VersionId.eq(next_model.version_id.clone()))
                        .exec(txn)
                        .await
                        .map_err(S3Error::internal_error)?;
                }
            }

            Ok((Some(model), false))
        } else {
            // No version_id: check bucket versioning status
            let bucket_model = Self::get_bucket_txn(txn, bucket).await?;
            let status = bucket_model.versioning_status;
            if status.is_none() {
                // Non-versioned: permanent delete null
                let m = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .filter(entity::object::Column::VersionId.eq("null"))
                    .one(txn)
                    .await
                    .map_err(S3Error::internal_error)?;
                if let Some(model) = m.clone() {
                    entity::object::Entity::delete_many()
                        .filter(entity::object::Column::BucketId.eq(bucket))
                        .filter(entity::object::Column::Id.eq(key))
                        .filter(entity::object::Column::VersionId.eq("null"))
                        .exec(txn)
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
                    .exec(txn)
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
                    .exec(txn)
                    .await
                    .map_err(S3Error::internal_error)?;

                // Return marker info: caller can create response with version_id
                // We need to fetch the marker to return? We'll synthesize.
                let marker_model = entity::object::Entity::find()
                    .filter(entity::object::Column::BucketId.eq(bucket))
                    .filter(entity::object::Column::Id.eq(key))
                    .filter(entity::object::Column::VersionId.eq(version_id.clone()))
                    .one(txn)
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

    /// Build the bounded page query for [`Self::list_object_versions`].
    ///
    /// `limit` is the SQL `LIMIT` (the caller's fetch cap) and
    /// `version_cursor` the resolved `(key_marker, last_modified)` position of
    /// `version_id_marker`, when both markers were supplied and resolved.
    /// Split out so the rendered SQL — `LIMIT` and marker predicate included —
    /// is assertable without a database.
    fn object_versions_page_query(
        bucket: &str,
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_cursor: Option<(String, chrono::DateTime<chrono::Utc>)>,
        limit: u64,
    ) -> sea_orm::Select<entity::object::Entity> {
        entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            // "Strictly after the cursor": rows past the marker key, plus the
            // marker key's own versions older than the cursor version.
            .filter(match version_cursor {
                Some((km, marker_modified)) => Condition::any()
                    .add(entity::object::Column::Id.gt(km.clone()))
                    .add(
                        Condition::all()
                            .add(entity::object::Column::Id.eq(km))
                            .add(entity::object::Column::LastModified.lt(marker_modified)),
                    )
                    .into_condition(),
                None => key_marker
                    .map(|km| entity::object::Column::Id.gt(km).into_condition())
                    .unwrap_or_else(|| Condition::all().into_condition()),
            })
            .order_by_asc(entity::object::Column::Id)
            .order_by_desc(entity::object::Column::LastModified)
            // Final tiebreaker: makes the page order total, so the marker
            // predicate above lands on the same boundary the walk starts from
            // even when two versions share a `last_modified`.
            .order_by_asc(entity::object::Column::VersionId)
            .limit(limit)
            .apply_if(prefix, |query, p| {
                query.filter(entity::object::Column::Id.starts_with(p))
            })
    }

    /// List one page of a bucket's object versions.
    ///
    /// Rows come back ordered `Id ASC, LastModified DESC, VersionId ASC` — the
    /// S3 version-listing order — and the page is bounded by a SQL `LIMIT`: at
    /// most `max_keys` rows are ever materialised, so a recursive
    /// `ListObjectVersions` walk costs one indexed page per request instead of
    /// a full-table materialisation (heavy `content` / `user_metadata` /
    /// `tags` / `checksums` columns included) per page.
    ///
    /// Pagination is a SQL predicate, not a post-fetch filter, so rows at or
    /// before the marker are never read. With only `key_marker` the cursor is
    /// the key itself; with `key_marker` + `version_id_marker` it is that
    /// exact row, and the rows after it are the ones with a greater key or a
    /// smaller `last_modified`. An unknown `version_id_marker` degrades to the
    /// key-only boundary.
    ///
    /// Callers pass `max_keys + 1` and use the extra row as the truncation
    /// sentinel: `len() > max_keys` means another page exists, the sentinel
    /// row is dropped, and the last returned row supplies the continuation
    /// `(key_marker, version_id_marker)`.
    ///
    /// `delimiter` is intentionally ignored: the handler folds the returned
    /// page into `CommonPrefix` entries, so the page boundary — not the bucket
    /// — is what bounds that rollup, exactly as when this query was unbounded.
    ///
    /// Returns `(versions, delete_markers)`, both split out of the single
    /// ordered page.
    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<String>,
        _delimiter: Option<String>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
        max_keys: Option<i32>,
    ) -> S3Result<(Vec<entity::object::Model>, Vec<entity::object::Model>)> {
        let limit = usize::try_from(max_keys.unwrap_or(MAX_KEYS_CEILING))
            .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;

        // Resolve the version cursor to its sort position first: a primary-key
        // probe, so the page query below never loads rows at/before the
        // marker just to find where to cut.
        let version_cursor = match (key_marker.as_deref(), version_id_marker.as_deref()) {
            (Some(km), Some(vid)) => match self.get_object_version(bucket, km, vid).await {
                Ok(model) => Some((km.to_owned(), model.last_modified)),
                Err(e) if *e.code() == S3ErrorCode::NoSuchKey => None,
                Err(e) => return Err(e),
            },
            _ => None,
        };

        let query = Self::object_versions_page_query(
            bucket,
            prefix.as_deref(),
            key_marker.as_deref(),
            version_cursor,
            limit as u64,
        );

        let page = query.all(&self.db).await.map_err(S3Error::internal_error)?;

        // Split into versions vs delete markers, preserving page order.
        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        for m in page {
            if m.is_delete_marker {
                delete_markers.push(m);
            } else {
                versions.push(m);
            }
        }

        Ok((versions, delete_markers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::repo::PutCondition;

    async fn repo() -> Repository {
        let db = sea_orm::Database::connect("sqlite::memory:")
            .await
            .expect("connect");
        Repository::init(db).await.expect("init")
    }

    /// Seed a versioned bucket with `keys`, one version each.
    async fn versioned_bucket_with_keys(repo: &Repository, keys: &[&str]) {
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");
        for key in keys {
            repo.cas_put_object(
                "b".into(),
                (*key).to_string(),
                ObjectWrite {
                    size: 1,
                    content_type: None,
                    etag: Some(format!("etag-{key}")),
                    content: serde_json::json!({"item": []}),
                    user_metadata: serde_json::json!({}),
                    checksums: serde_json::json!({}),
                    tags: serde_json::json!([]),
                },
                PutCondition::None,
            )
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
        }
    }

    fn page_sql(
        prefix: Option<&str>,
        key_marker: Option<&str>,
        version_cursor: Option<(String, chrono::DateTime<chrono::Utc>)>,
        limit: u64,
    ) -> String {
        Repository::object_versions_page_query("b", prefix, key_marker, version_cursor, limit)
            .build(sea_orm::DbBackend::Sqlite)
            .to_string()
    }

    #[test]
    fn version_page_query_carries_sql_limit() {
        let sql = page_sql(None, None, None, 3);
        assert!(
            sql.contains("LIMIT 3"),
            "version page query must bound rows in SQL: {sql}"
        );
    }

    #[test]
    fn version_page_query_pushes_markers_into_sql() {
        let cursor_at = chrono::DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);

        let sql = page_sql(Some("a/"), Some("a/2"), Some(("a/2".into(), cursor_at)), 5);
        assert!(sql.contains("LIMIT 5"), "missing limit: {sql}");
        assert!(
            sql.contains(r#""id" > $1"#) || sql.contains(r#""id" > "#),
            "key marker must be a SQL predicate, not a post-fetch filter: {sql}"
        );
        assert!(
            sql.contains(r#""last_modified" <"#),
            "version marker must be a SQL predicate: {sql}"
        );
        assert!(
            sql.contains(r#""id" LIKE"#) || sql.contains("LIKE"),
            "prefix must stay in SQL: {sql}"
        );

        // The version cursor is only meaningful together with its key.
        let sql = page_sql(None, Some("a/2"), None, 5);
        assert!(
            !sql.contains(r#""last_modified" <"#),
            "key-only marker must not emit a last_modified bound: {sql}"
        );
    }

    #[tokio::test]
    async fn list_object_versions_bounds_rows_to_limit() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "a/3", "b/1", "b/2"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(2))
            .await
            .expect("page 1");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/1", "a/2"],
            "SQL limit must cap the returned page"
        );
    }

    #[tokio::test]
    async fn version_page_two_starts_after_both_markers() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "a/3", "b/1", "b/2"]).await;

        let (versions, page1_markers) = repo
            .list_object_versions("b", None, None, None, None, Some(2))
            .await
            .expect("page 1");
        assert!(page1_markers.is_empty());
        let last = versions.last().expect("page 1 must be non-empty");

        let (versions, markers) = repo
            .list_object_versions(
                "b",
                None,
                None,
                Some(last.id.clone()),
                Some(last.version_id.clone()),
                Some(2),
            )
            .await
            .expect("page 2");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/3", "b/1"],
            "continuation markers must resume at the next row, not repeat or skip"
        );
    }

    #[tokio::test]
    async fn version_key_marker_alone_skips_the_whole_marker_key() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, Some("a/1".into()), None, Some(10))
            .await
            .expect("list");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/2", "b/1"]
        );
    }

    #[tokio::test]
    async fn unknown_version_marker_degrades_to_key_boundary() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions(
                "b",
                None,
                None,
                Some("a/1".into()),
                Some("no-such-version".into()),
                Some(10),
            )
            .await
            .expect("list");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["b/1"],
            "an unresolvable version marker must not resurrect the marked key"
        );
    }

    #[tokio::test]
    async fn version_pages_of_zero_limit_return_nothing() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(0))
            .await
            .expect("list");
        assert!(versions.is_empty());
        assert!(markers.is_empty());
    }

    // ---- Atomicity of the versioned delete path ----

    /// Insert one version row with an explicit timestamp, so "most recent"
    /// is unambiguous without depending on clock resolution.
    async fn seed_version(repo: &Repository, version_id: &str, at: &str, is_latest: bool) {
        let last_modified = chrono::DateTime::parse_from_rfc3339(at)
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        let active = entity::object::ActiveModel {
            bucket_id: Set("b".into()),
            id: Set("k".into()),
            version_id: Set(version_id.into()),
            is_latest: Set(is_latest),
            is_delete_marker: Set(false),
            size: Set(1),
            last_modified: Set(last_modified),
            content_type: Set(None),
            etag: Set(Some(format!("etag-{version_id}"))),
            user_metadata: Set(serde_json::json!({})),
            tags: Set(serde_json::json!([])),
            checksums: Set(serde_json::json!({})),
            content: Set(serde_json::json!({"item": []})),
        };
        entity::object::Entity::insert(active)
            .exec(&repo.db)
            .await
            .expect("seed version");
    }

    async fn versioned_bucket(repo: &Repository) {
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");
    }

    async fn rows_for_key(repo: &Repository) -> Vec<entity::object::Model> {
        entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq("b"))
            .filter(entity::object::Column::Id.eq("k"))
            .all(&repo.db)
            .await
            .expect("load rows")
    }

    #[tokio::test]
    async fn deleting_latest_version_promotes_newest_predecessor() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", false).await;
        seed_version(&repo, "v2", "2024-01-02T00:00:00Z", false).await;
        seed_version(&repo, "v3", "2024-01-03T00:00:00Z", true).await;

        let (deleted, marker) = repo
            .delete_object_versioned("b", "k", Some("v3"))
            .await
            .expect("delete latest");
        assert_eq!(
            deleted.map(|m| m.version_id).as_deref(),
            Some("v3"),
            "permanent delete returns the removed row"
        );
        assert!(!marker, "permanent delete creates no delete marker");

        let rows = rows_for_key(&repo).await;
        let latest: Vec<_> = rows.iter().filter(|m| m.is_latest).collect();
        assert_eq!(
            latest.iter().map(|m| m.version_id.as_str()).collect::<Vec<_>>(),
            ["v2"],
            "the newest remaining version must be promoted"
        );

        // The promoted row is the one every later read resolves to.
        let got = repo.get_object("b", "k").await.expect("get promoted object");
        assert_eq!(got.version_id, "v2");
        assert_eq!(got.etag.as_deref(), Some("etag-v2"));
    }

    #[tokio::test]
    async fn deleting_the_last_remaining_version_leaves_no_latest() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", true).await;

        repo.delete_object_versioned("b", "k", Some("v1"))
            .await
            .expect("delete only version");

        let rows = rows_for_key(&repo).await;
        assert!(rows.is_empty(), "the only version row is gone");
        assert!(
            rows.iter().all(|m| !m.is_latest),
            "no orphan latest row may remain"
        );
    }

    #[tokio::test]
    async fn deleting_the_newest_version_leaves_object_readable() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", false).await;
        seed_version(&repo, "v2", "2024-01-02T00:00:00Z", true).await;

        // A plain DELETE plants a delete marker; the key becomes unreadable
        // but keeps exactly one latest row (the marker).
        let (marker_model, is_marker) = repo
            .delete_object_versioned("b", "k", None)
            .await
            .expect("delete without version id");
        assert!(is_marker, "versioned bucket must create a delete marker");
        let marker_model = marker_model.expect("delete marker model");
        assert!(marker_model.is_delete_marker && marker_model.is_latest);

        let latest = rows_for_key(&repo)
            .await
            .into_iter()
            .filter(|m| m.is_latest)
            .count();
        assert_eq!(latest, 1, "the delete marker is the single latest row");

        // Removing the marker (the newest version) exposes v2 again.
        repo.delete_object_versioned("b", "k", Some(&marker_model.version_id))
            .await
            .expect("delete the marker");

        let got = repo.get_object("b", "k").await.expect("object is readable");
        assert_eq!(got.version_id, "v2");
        assert_eq!(
            rows_for_key(&repo)
                .await
                .iter()
                .filter(|m| m.is_latest)
                .count(),
            1,
            "promotion after marker removal leaves exactly one latest"
        );
    }

    #[tokio::test]
    async fn unknown_version_delete_is_a_no_op_inside_the_transaction() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", true).await;

        let err = repo
            .delete_object_versioned("b", "k", Some("nope"))
            .await
            .expect_err("unknown version must fail");
        assert!(
            format!("{err:?}").contains("NoSuchKey"),
            "expected NoSuchKey, got {err:?}"
        );

        // The rolled-back transaction must not have touched anything.
        let rows = rows_for_key(&repo).await;
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_latest, "v1 stays latest after a failed delete");
    }

    #[tokio::test]
    async fn concurrent_mixed_put_and_delete_converge_to_one_latest() {
        use futures::future::join_all;

        let repo = std::sync::Arc::new(repo().await);
        versioned_bucket(&repo).await;

        // Same shape as the concurrent-put harness in `super::tests`, but with
        // transacted puts interleaved against transacted versioned deletes.
        // Both paths must move `is_latest` atomically, or the key ends up with
        // zero (unreadable) or two (arbitrary) latest rows.
        let futs = (0..10).map(|i| {
            let repo = repo.clone();
            async move {
                if i % 2 == 0 {
                    repo.cas_put_object(
                        "b".into(),
                        "k".into(),
                        ObjectWrite {
                            size: 1,
                            content_type: None,
                            etag: Some(format!("e{i}")),
                            content: serde_json::json!({"item": []}),
                            user_metadata: serde_json::json!({}),
                            checksums: serde_json::json!({}),
                            tags: serde_json::json!([]),
                        },
                        PutCondition::None,
                    )
                    .await
                    .map(|_| ())
                } else {
                    // Permanent delete of a version id that may not exist yet,
                    // or a delete-marker create; both must keep the key
                    // resolvable to exactly one latest row.
                    if i % 4 == 1 {
                        repo.delete_object_versioned("b", "k", None)
                            .await
                            .map(|_| ())
                    } else {
                        match repo.delete_object_versioned("b", "k", Some("missing")).await {
                            // An unknown version id is NoSuchKey by contract;
                            // the transaction rolls back and changes nothing.
                            Err(_) => Ok(()),
                            Ok(_) => panic!("deleting a missing version must fail"),
                        }
                    }
                }
            }
        });
        let results = join_all(futs).await;
        assert!(
            results.iter().all(|r| r.is_ok()),
            "all concurrent operations should succeed: {results:?}"
        );

        let rows = rows_for_key(&repo).await;
        assert_eq!(
            rows.iter().filter(|m| m.is_latest).count(),
            1,
            "exactly one latest row must survive mixed put/delete: {rows:?}"
        );

        // Whatever row won must be the one every read resolves to.
        let latest = rows.iter().find(|m| m.is_latest).expect("a latest row");
        match repo.get_latest_raw("b", "k").await {
            Ok(m) => {
                assert_eq!(m.map(|m| m.version_id), Some(latest.version_id.clone()));
            }
            Err(e) => panic!("latest read failed: {e:?}"),
        }
    }

    // ---- Atomicity of the blob + object publish ----

    fn write_with_content(content: serde_json::Value, etag: &str) -> ObjectWrite {
        ObjectWrite {
            size: 1,
            content_type: None,
            etag: Some(etag.into()),
            content,
            user_metadata: serde_json::json!({}),
            checksums: serde_json::json!({}),
            tags: serde_json::json!([]),
        }
    }

    fn content_for(blob_id: &str) -> serde_json::Value {
        serde_json::json!({"item": [{"id": blob_id, "offset": 0, "size": 1}]})
    }

    async fn blob_rows(repo: &Repository) -> Vec<entity::blob::Model> {
        entity::blob::Entity::find()
            .all(&repo.db)
            .await
            .expect("load blob rows")
    }

    #[tokio::test]
    async fn publish_records_the_blob_row_and_the_object_row_together() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let version_id = repo
            .publish_object_with_new_blob(
                "blob-1".into(),
                1,
                "b".into(),
                "k".into(),
                write_with_content(content_for("blob-1"), "e1"),
                PutCondition::None,
            )
            .await
            .expect("publish");

        assert_eq!(version_id, "null");
        let blobs = blob_rows(&repo).await;
        assert_eq!(blobs.len(), 1, "the blob row must exist");
        assert_eq!(blobs[0].id, "blob-1");
        assert_eq!(blobs[0].refs, 1);

        let object = repo.get_object("b", "k").await.expect("published object");
        assert_eq!(object.content, content_for("blob-1"));
    }

    #[tokio::test]
    async fn failed_publish_leaves_no_orphaned_blob_row() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.cas_put_object(
            "b".into(),
            "k".into(),
            write_with_content(serde_json::json!({"item": []}), "e1"),
            PutCondition::None,
        )
        .await
        .expect("seed object");

        // Second step of the publish fails on the condition check; the blob
        // row inserted moments earlier in the same transaction must go with
        // it instead of surviving as a `refs = 1` row nothing references.
        let err = repo
            .publish_object_with_new_blob(
                "blob-orphan".into(),
                1,
                "b".into(),
                "k".into(),
                write_with_content(content_for("blob-orphan"), "e2"),
                PutCondition::IfNoneMatchAny,
            )
            .await
            .expect_err("precondition must fail");
        assert!(
            format!("{err:?}").contains("PreconditionFailed"),
            "expected PreconditionFailed, got {err:?}"
        );

        assert!(
            blob_rows(&repo).await.is_empty(),
            "a failed publish must not leave a blob row behind"
        );
        let object = repo.get_object("b", "k").await.expect("object unchanged");
        assert_eq!(object.etag.as_deref(), Some("e1"));
    }

    #[tokio::test]
    async fn publish_into_a_missing_bucket_leaves_no_blob_row() {
        let repo = repo().await;

        let err = repo
            .publish_object_with_new_blob(
                "blob-orphan".into(),
                1,
                "no-such-bucket".into(),
                "k".into(),
                write_with_content(content_for("blob-orphan"), "e1"),
                PutCondition::None,
            )
            .await
            .expect_err("missing bucket must fail");
        assert!(
            format!("{err:?}").contains("NoSuchBucket"),
            "expected NoSuchBucket, got {err:?}"
        );

        assert!(
            blob_rows(&repo).await.is_empty(),
            "the rolled-back publish must not leave a blob row behind"
        );
    }

    #[tokio::test]
    async fn blob_id_is_rejected_before_the_object_is_written() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        // An id that is already registered cannot be re-registered: the
        // publish transaction must fail as a whole rather than overwrite the
        // existing row's ownership.
        repo.register_new_blob("blob-1".into(), 1)
            .await
            .expect("register");

        repo.publish_object_with_new_blob(
            "blob-1".into(),
            1,
            "b".into(),
            "k".into(),
            write_with_content(content_for("blob-1"), "e1"),
            PutCondition::None,
        )
        .await
        .expect_err("duplicate blob id must fail");

        let blobs = blob_rows(&repo).await;
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].refs, 1, "the existing row is untouched");
        assert!(
            !repo.object_exists("b", "k").await.expect("exists"),
            "no object row may survive a failed publish"
        );
    }
}
