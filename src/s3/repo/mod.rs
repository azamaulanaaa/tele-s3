use s3s::{S3Error, S3ErrorCode, S3Result};
use sea_orm::{DatabaseConnection, DbErr};
use tracing::instrument;

mod blobs;
mod buckets;
mod multipart;
mod objects;

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
    /// Object tag-set as JSON array of {key, value}; empty array when none.
    pub tags: serde_json::Value,
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

    /// The `size` columns are 32-bit. Reject anything larger instead of
    /// silently truncating it with `as u32`.
    fn checked_size(size: u64) -> S3Result<u32> {
        u32::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::EntityTooLarge))
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
                    tags: serde_json::json!([]),
                },
                PutCondition::IfNoneMatchAny,
            )
            .await;
        assert!(
            first.is_ok(),
            "first create-only put should succeed: {first:?}"
        );

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
                    tags: serde_json::json!([]),
                },
                PutCondition::IfNoneMatchAny,
            )
            .await;

        match second {
            Ok(_) => panic!("second create-only put should fail"),
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
