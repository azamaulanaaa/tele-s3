use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::{ColumnTrait, DbErr, EntityTrait, PaginatorTrait, QueryFilter, Set, SqlErr};
use tracing::instrument;

use super::Repository;
use super::entity;

impl Repository {
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

    #[instrument(skip(self), level = "debug", err)]
    pub async fn get_bucket(&self, name: &str) -> S3Result<entity::bucket::Model> {
        let bucket = entity::bucket::Entity::find_by_id(name)
            .one(&self.db)
            .await
            .map_err(S3Error::internal_error)?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?;

        Ok(bucket)
    }
}
