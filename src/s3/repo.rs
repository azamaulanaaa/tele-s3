use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, SqlErr, sea_query::OnConflict,
};

use super::entity;

pub struct Repository {
    pub db: DatabaseConnection,
}

impl Repository {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

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

    pub async fn list_buckets(&self) -> S3Result<Vec<entity::bucket::Model>> {
        let buckets = entity::bucket::Entity::find()
            .all(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(buckets)
    }

    pub async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        entity::bucket::Entity::delete_by_id(name)
            .exec(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(())
    }

    pub async fn get_bucket_object_count(&self, name: &str) -> S3Result<u64> {
        let object_count = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(name))
            .count(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(object_count)
    }

    pub async fn bucket_exists(&self, name: &str) -> S3Result<bool> {
        let bucket_exists = entity::bucket::Entity::find_by_id(name)
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .is_some();

        Ok(bucket_exists)
    }

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
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(())
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> S3Result<entity::object::Model> {
        let model = entity::object::Entity::find_by_id((bucket.to_string(), key.to_string()))
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        Ok(model)
    }

    pub async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<entity::object::Model>> {
        let model = entity::object::Entity::delete_by_id((bucket.to_string(), key.to_string()))
            .exec_with_returning(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(model)
    }

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
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(models)
    }

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
            .map_err(|e| S3Error::internal_error(e))?;

        Ok(models)
    }
}
