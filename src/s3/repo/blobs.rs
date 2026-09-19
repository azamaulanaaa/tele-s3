use s3s::{S3Error, S3Result};
use sea_orm::prelude::Expr;
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
    ///
    /// Implemented as a single atomic upsert
    /// (`INSERT ... ON CONFLICT DO UPDATE SET refs = refs + 1`), so
    /// concurrent acquirers cannot lose increments the way a
    /// check-then-insert can. A previously unknown blob is seeded with a
    /// single reference owned by the caller acquiring it.
    #[instrument(skip(self, items), level = "debug", err)]
    pub async fn acquire_blob_refs(&self, items: &[(String, u64)]) -> S3Result<()> {
        use sea_orm::{ConnectionTrait, DbBackend, Statement};

        let backend = self.db.get_database_backend();
        for (id, size) in items {
            let size32 = Self::checked_size(*size)?;
            let now = chrono::Local::now().to_utc();
            let values = [
                sea_orm::Value::from(id.clone()),
                sea_orm::Value::from(size32),
                sea_orm::Value::from(now),
            ];
            let sql = match backend {
                DbBackend::MySql => "INSERT INTO `s3_blob` (`id`, `size`, `refs`, `created_at`) VALUES (?, ?, 1, ?) ON DUPLICATE KEY UPDATE `refs` = `refs` + 1".to_string(),
                DbBackend::Postgres => "INSERT INTO \"s3_blob\" (\"id\", \"size\", \"refs\", \"created_at\") VALUES ($1, $2, 1, $3) ON CONFLICT (\"id\") DO UPDATE SET \"refs\" = \"s3_blob\".\"refs\" + 1".to_string(),
                _ => "INSERT INTO \"s3_blob\" (\"id\", \"size\", \"refs\", \"created_at\") VALUES (?, ?, 1, ?) ON CONFLICT (\"id\") DO UPDATE SET \"refs\" = \"s3_blob\".\"refs\" + 1".to_string(),
            };

            self.db
                .execute_raw(Statement::from_sql_and_values(backend, sql, values))
                .await
                .map_err(S3Error::internal_error)?;
        }

        Ok(())
    }

    /// Drop one reference for each listed blob id.
    #[instrument(skip(self, ids), level = "debug", err)]
    pub async fn release_blob_refs(&self, ids: &[String]) -> S3Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        // The same blob may back several slices of one object (e.g.
        // repeated UploadPartCopy ranges); each slice holds its own
        // reference, so count occurrences instead of decrementing once.
        let mut counts: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for id in ids {
            *counts.entry(id.as_str()).or_default() += 1;
        }

        for (id, n) in &counts {
            entity::blob::Entity::update_many()
                .col_expr(
                    entity::blob::Column::Refs,
                    Expr::col(entity::blob::Column::Refs).sub(*n),
                )
                .filter(entity::blob::Column::Id.eq(*id))
                .exec(&self.db)
                .await
                .map_err(S3Error::internal_error)?;
        }

        let distinct: Vec<String> = counts.keys().map(|s| s.to_string()).collect();

        // Only collect rows for the ids this call released; other
        // zero-ref rows (if any) belong to other callers.
        let released = entity::blob::Entity::find()
            .filter(entity::blob::Column::Id.is_in(distinct.clone()))
            .filter(entity::blob::Column::Refs.lte(0))
            .all(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        if released.is_empty() {
            return Ok(Vec::new());
        }

        entity::blob::Entity::delete_many()
            .filter(entity::blob::Column::Id.is_in(distinct))
            .filter(entity::blob::Column::Refs.lte(0))
            .exec(&self.db)
            .await
            .map_err(S3Error::internal_error)?;

        Ok(released.into_iter().map(|model| model.id).collect())
    }
}
