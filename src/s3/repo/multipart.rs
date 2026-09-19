use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, Set};
use tracing::instrument;

use super::Repository;
use super::entity;

impl Repository {
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
