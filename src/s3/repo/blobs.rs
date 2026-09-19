use s3s::{S3Error, S3Result};
use sea_orm::prelude::Expr;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, EntityTrait, ExprTrait, QueryFilter, Set};
use tracing::instrument;

use super::Repository;
use super::entity;

impl Repository {
    /// Register a freshly created backend blob with a single reference.
    #[instrument(skip(self), level = "debug", err)]
    pub async fn register_new_blob(&self, id: String, size: u64) -> S3Result<()> {
        let active_model = entity::blob::ActiveModel {
            id: Set(id),
            size: Set(Self::checked_size(size)?),
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
                    size: Set(Self::checked_size(*size)?),
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
}
