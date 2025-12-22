use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, SqlErr, sea_query::OnConflict,
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
        marker: Option<String>,
        limit: u64,
    ) -> S3Result<Vec<entity::object::Model>> {
        let mut query = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket))
            .order_by_asc(entity::object::Column::Id);

        if let Some(prefix) = prefix {
            query = query.filter(entity::object::Column::Id.starts_with(prefix.clone()));
        }

        if let Some(marker) = marker {
            query = query.filter(entity::object::Column::Id.gt(marker.clone()));
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
    pub async fn update_multipart_upload_state<F>(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        action: F,
    ) -> S3Result<()>
    where
        F: Fn(
            entity::multipart_upload_state::Model,
        ) -> S3Result<entity::multipart_upload_state::ActiveModel>,
    {
        let model = entity::multipart_upload_state::Entity::find_by_id((
            bucket.to_string(),
            key.to_string(),
            upload_id.to_string(),
        ))
        .one(&self.db)
        .await
        .map_err(S3Error::internal_error)?
        .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        let model = action(model)?;

        model
            .update(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(())
    }
}
