use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, DbErr, EntityTrait, ExprTrait,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, Set, SqlErr,
    prelude::Expr,
    sea_query::{OnConflict, Query},
};
use tracing::instrument;

pub mod entity;

pub struct Repository {
    pub db: DatabaseConnection,
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

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_bucket(&self, name: &str) -> S3Result<entity::bucket::Model> {
        let bucket = entity::bucket::Entity::find_by_id(name)
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?;

        Ok(bucket)
    }

    #[instrument(skip(self), level = "debug", err)]
    pub async fn upsert_object(
        &self,
        bucket: String,
        key: String,
        size: u64,
        content_type: Option<String>,
        etag: Option<String>,
        metadata: serde_json::Value,
    ) -> S3Result<()> {
        let active_model = entity::object::ActiveModel {
            bucket_id: Set(bucket),
            id: Set(key),
            size: Set(size as u32),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(content_type),
            etag: Set(etag),
            content: Set(metadata),
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
        content: serde_json::Value,
    ) -> S3Result<()> {
        let active_model = entity::multipart_upload_state::ActiveModel {
            bucket_id: Set(bucket),
            object_id: Set(key),
            upload_id: Set(upload_id),
            content_type: Set(content_type),
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
