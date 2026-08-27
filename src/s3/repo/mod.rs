use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DbBackend, DbErr, EntityTrait,
    ExprTrait, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, SqlErr, Statement,
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

    #[instrument(skip(self), level = "debug", err)]
    pub async fn create_bucket(&self, name: String, region: Option<String>) -> S3Result<()> {
        let active_model = entity::bucket::ActiveModel {
            id: Set(name),
            region: Set(region),
            created_at: Set(chrono::Local::now().to_utc()),
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

    /// Compare-and-swap object write.
    ///
    /// Every branch lands as a single atomic SQL statement whose guard
    /// lives in the WHERE clause, so racing conditional writers serialize
    /// at the database level without explicit transactions — an abandoned
    /// open transaction on a precondition failure would otherwise race
    /// follow-up writes and mask the 412 as an InternalError. Failing
    /// conditions return 412 PreconditionFailed (or NoSuchKey for If-Match
    /// on a missing object) leaving the previous state untouched.
    #[instrument(skip(self, data, condition), level = "debug", err)]
    pub async fn cas_put_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<()> {
        let now = chrono::Local::now().to_utc();

        const SET_SQL: &str = "UPDATE \"s3_object\" SET size = ?, last_modified = ?, \
             content_type = ?, etag = ?, content = ?, user_metadata = ?, checksums = ?";

        match condition {
            PutCondition::None => {
                self.upsert_object(bucket, key, data).await
            }
            PutCondition::IfNoneMatchAny => {
                let res = self
                    .db
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "INSERT INTO \"s3_object\" (bucket_id, id, size, last_modified, \
                         content_type, etag, content, user_metadata, tags, checksums) \
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                         ON CONFLICT (bucket_id, id) DO NOTHING",
                        [
                            bucket.into(),
                            key.into(),
                            (data.size as i64).into(),
                            now.into(),
                            data.content_type.into(),
                            data.etag.into(),
                            data.content.into(),
                            data.user_metadata.into(),
                            serde_json::json!([]).into(),
                            data.checksums.into(),
                        ],
                    ))
                    .await
                    .map_err(S3Error::internal_error)?;

                if res.rows_affected() == 0 {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }

                Ok(())
            }
            PutCondition::IfMatch(expected) => {
                let res = self
                    .db
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        format!(
                            "{SET_SQL} WHERE bucket_id = ? AND id = ? AND etag = ?"
                        ),
                        [
                            (data.size as i64).into(),
                            now.into(),
                            data.content_type.clone().into(),
                            data.etag.clone().into(),
                            data.content.clone().into(),
                            data.user_metadata.clone().into(),
                            data.checksums.clone().into(),
                            bucket.as_str().into(),
                            key.as_str().into(),
                            expected.as_str().into(),
                        ],
                    ))
                    .await
                    .map_err(S3Error::internal_error)?;

                if res.rows_affected() == 1 {
                    return Ok(());
                }

                if !self.object_exists(&bucket, &key).await? {
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }

                Err(S3Error::new(S3ErrorCode::PreconditionFailed))
            }
            PutCondition::IfMatchAny => {
                let res = self
                    .db
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        format!(
                            "{SET_SQL} WHERE bucket_id = ? AND id = ?"
                        ),
                        [
                            (data.size as i64).into(),
                            now.into(),
                            data.content_type.into(),
                            data.etag.into(),
                            data.content.into(),
                            data.user_metadata.into(),
                            data.checksums.into(),
                            bucket.as_str().into(),
                            key.as_str().into(),
                        ],
                    ))
                    .await
                    .map_err(S3Error::internal_error)?;

                if res.rows_affected() == 0 {
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }

                Ok(())
            }
            PutCondition::IfNoneMatch(expected) => {
                // The guarded update only lands when the current ETag differs;
                // falling through means the object is absent (create it) or
                // already carries the forbidden digest (412).
                let res = self
                    .db
                    .execute_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        format!(
                            "{SET_SQL} WHERE bucket_id = ? AND id = ? \
                             AND (etag IS NULL OR etag <> ?)"
                        ),
                        [
                            (data.size as i64).into(),
                            now.into(),
                            data.content_type.clone().into(),
                            data.etag.clone().into(),
                            data.content.clone().into(),
                            data.user_metadata.clone().into(),
                            data.checksums.clone().into(),
                            bucket.as_str().into(),
                            key.as_str().into(),
                            expected.as_str().into(),
                        ],
                    ))
                    .await
                    .map_err(S3Error::internal_error)?;

                if res.rows_affected() == 1 {
                    return Ok(());
                }

                if !self.object_exists(&bucket, &key).await? {
                    return self.upsert_object(bucket, key, data).await;
                }

                Err(S3Error::new(S3ErrorCode::PreconditionFailed))
            }
        }
    }

    /// Register a freshly created backend blob with a single reference.
    ///
    /// Called immediately after a successful backend write, when the caller
    /// is guaranteed to be the blob's first and only owner.
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
    ///
    /// Rows are created on first sight with refs=2 rather than 1: besides
    /// the newly acquired reference this counts a possible pre-existing
    /// reference from an object written before tracking existed (legacy
    /// objects carry no blob record). Over-counting only keeps blobs alive
    /// longer; under-counting would corrupt surviving aliases.
    #[instrument(skip(self, items), level = "debug", err)]
    pub async fn acquire_blob_refs(&self, items: &[(String, u64)]) -> S3Result<()> {
        for (id, size) in items {
            let existing = entity::blob::Entity::find_by_id(id)
                .one(&self.db)
                .await
                .map_err(S3Error::internal_error)?;

            if existing.is_some() {
                // Server-side arithmetic so concurrent acquires can't lose
                // updates between read and write.
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
    ///
    /// Returns the ids whose reference count reached zero; callers must
    /// delete those blobs from the backend. Ids without a tracking row are
    /// ignored: hard-deleting them could corrupt an alias that predates
    /// tracking, whereas leaking an unreferenced backend blob is only a
    /// storage cost.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn release_blob_refs(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // Server-side decrement so concurrent releases can't lose updates.
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

    /// Replace the tag set stored on an object. The object must exist;
    /// a missing key yields NoSuchKey.
    #[instrument(skip(self, tags), level = "debug", err)]
    pub async fn set_object_tags(
        &self,
        bucket: &str,
        key: &str,
        tags: serde_json::Value,
    ) -> S3Result<()> {
        let result = entity::object::Entity::update_many()
            .col_expr(entity::object::Column::Tags, Expr::value(tags))
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.eq(key))
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

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
    pub async fn upsert_object(&self, bucket: String, key: String, data: ObjectWrite) -> S3Result<()> {
        let active_model = entity::object::ActiveModel {
            bucket_id: Set(bucket),
            id: Set(key),
            size: Set(data.size as u32),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(data.content_type),
            etag: Set(data.etag),
            user_metadata: Set(data.user_metadata),
            tags: Set(serde_json::json!([])),
            checksums: Set(data.checksums),
            content: Set(data.content),
        };

        entity::object::Entity::insert(active_model)
            .on_conflict(
                OnConflict::columns([entity::object::Column::BucketId, entity::object::Column::Id])
                    .update_columns([
                        entity::object::Column::Size,
                        entity::object::Column::LastModified,
                        entity::object::Column::ContentType,
                        entity::object::Column::Etag,
                        entity::object::Column::Content,
                        // Overwrites replace metadata, but tags persist:
                        // tagging is managed via its own API.
                        entity::object::Column::UserMetadata,
                        entity::object::Column::Checksums,
                    ])
                    .to_owned(),
            )
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn object_exists(&self, bucket: &str, key: &str) -> S3Result<bool> {
        let exists = entity::object::Entity::find_by_id((bucket.to_string(), key.to_string()))
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .is_some();

        Ok(exists)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<entity::object::Model> {
        let model = entity::object::Entity::find_by_id((bucket.to_string(), key.to_string()))
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        Ok(model)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<entity::object::Model>> {
        let model = entity::object::Entity::delete_by_id((bucket.to_string(), key.to_string()))
            .exec_with_returning(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(model)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn delete_objects(
        &self,
        bucket: &str,
        keys: Vec<String>,
    ) -> S3Result<Vec<entity::object::Model>> {
        let models = entity::object::Entity::delete_many()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .filter(entity::object::Column::Id.is_in(keys.clone()))
            .exec_with_returning(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(models)
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
        let mut query = entity::object::Entity::find()
            .filter(
                Condition::all()
                    .add(entity::object::Column::BucketId.eq(bucket))
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
                                    // .expr(Expr::cust_with_exprs(
                                    //     "SUBSTR(MIN(?), 1, ? + INSTR(SUBSTR(MIN(?), ?), ?))",
                                    //     [
                                    //         entity::object::Column::Id.into_expr(),
                                    //         (prefix_len.saturating_sub(1)).into(),
                                    //         entity::object::Column::Id.into_expr(),
                                    //         prefix_len.into(),
                                    //         delimiter.clone().into(),
                                    //     ],
                                    // ))
                                    .from(entity::object::Entity)
                                    .distinct()
                                    .cond_where(
                                        Condition::all()
                                            .add(entity::object::Column::BucketId.eq(bucket))
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

    /// Compare-and-swap update of a multipart upload state's content.
    ///
    /// The closure mutates a snapshot of the stored content; the result is
    /// swapped in only if the stored value is unchanged since the snapshot
    /// (optimistic locking). Conflicting concurrent updates retry from a
    /// fresh snapshot, bounded before surfacing an error. This closes the
    /// lost-update race where two parts uploaded concurrently could
    /// overwrite each other's metadata.
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

            // Closure changed nothing → nothing to swap; treat as success
            // so idempotent retries converge.
            if new_content == model.content {
                return Ok(());
            }

            // Conditional swap: only succeeds when the stored content is
            // identical to the snapshot the change was based on. JSON
            // serialization is deterministic here (BTreeMap + serde), so
            // value equality is a reliable change detector.
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

            // Someone updated the state between our read and write; retry
            // from a fresh snapshot.
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

        // s3_object carries a foreign key to s3_bucket; the bucket row must
        // exist before any object insert can land.
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

        // Second create-only put must be rejected with PreconditionFailed.
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
            Ok(()) => panic!("second create-only put should fail"),
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
