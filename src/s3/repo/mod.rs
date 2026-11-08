use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::instrument;

mod blobs;
mod buckets;
mod multipart;
mod objects;

pub(crate) use multipart::{MultipartCompletion, MultipartUploadStateUpdate};

use crate::storage::Store;

/// Precondition for a compare-and-swap object write.
pub(crate) enum PutCondition {
    None,
    /// If-Match with a specific `ETag`: write only if the object exists
    /// and its stored `ETag` matches. Missing object → `NoSuchKey`.
    IfMatch(String),
    /// If-Match: * — write only if the object exists.
    IfMatchAny,
    /// If-None-Match: * — create-only. Any existing object returns 412.
    IfNoneMatchAny,
    /// If-None-Match with a specific `ETag`: fail only if the object exists
    /// and its stored `ETag` matches.
    IfNoneMatch(String),
}

/// S3 metadata repository over one shared redb store.
pub(crate) struct Repository {
    pub(crate) store: Store,
}

/// Field bundle for writing an object row.
pub(crate) struct ObjectWrite {
    pub size: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub content: serde_json::Value,
    /// x-amz-meta-* map as a JSON object. Empty object when none.
    pub user_metadata: serde_json::Value,
    /// Client-provided checksums as a JSON object keyed by algorithm.
    /// empty object when none.
    pub checksums: serde_json::Value,
    /// Object tag-set as JSON array of {key, value}. Empty array when none.
    pub tags: serde_json::Value,
}

impl Repository {
    #[instrument(skip(store), level = "debug")]
    pub(crate) async fn init(store: Store) -> anyhow::Result<Self> {
        Ok(Self { store })
    }

    /// The `size` columns are 32-bit. Reject anything larger instead of
    /// silently truncating it with `as u32`.
    pub(super) fn checked_size(size: u64) -> S3Result<u32> {
        u32::try_from(size).map_err(|_| S3Error::new(S3ErrorCode::EntityTooLarge))
    }

    /// Fail when a test fault is active. Always a no-op in production.
    pub(super) fn inject(&self, fault: crate::storage::Fault) -> S3Result<()> {
        if self.store.fault(fault) {
            return Err(S3Error::internal_error(std::io::Error::other(
                "injected storage failure",
            )));
        }

        Ok(())
    }
}

impl From<crate::storage::StoreError> for S3Error {
    fn from(err: crate::storage::StoreError) -> Self {
        S3Error::internal_error(err)
    }
}

/// Convert a redb or JSON result into an S3 result.
pub(super) trait StoreResultExt<T> {
    /// Map any stored error to an S3 internal error.
    fn store(self) -> S3Result<T>;
}

impl<T, E> StoreResultExt<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn store(self) -> S3Result<T> {
        self.map_err(S3Error::internal_error)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;
    use crate::storage::Store;

    async fn repo() -> Repository {
        let store = Store::in_memory().expect("store");
        Repository::init(store).await.expect("init")
    }

    fn write(etag: &str) -> ObjectWrite {
        ObjectWrite {
            size: 1,
            content_type: None,
            etag: Some(etag.into()),
            content: serde_json::json!({}),
            user_metadata: serde_json::json!({}),
            checksums: serde_json::json!({}),
            tags: serde_json::json!([]),
        }
    }

    #[tokio::test]
    async fn cas_put_if_none_match_any_rejects_second_create() {
        let repo = repo().await;

        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        repo.cas_put_object(
            "b".into(),
            "k".into(),
            write("e1"),
            PutCondition::IfNoneMatchAny,
        )
        .await
        .expect("first create-only put should succeed");

        let second = repo
            .cas_put_object(
                "b".into(),
                "k".into(),
                write("e2"),
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

    #[tokio::test]
    async fn concurrent_versioned_puts_leave_single_latest() {
        use futures::future::join_all;

        let repo = std::sync::Arc::new(repo().await);

        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");

        let futs = (0..10).map(|i| {
            let repo = repo.clone();
            async move {
                repo.cas_put_object(
                    "b".into(),
                    "k".into(),
                    write(&format!("e{i}")),
                    PutCondition::None,
                )
                .await
            }
        });
        let results = join_all(futs).await;
        assert!(
            results.iter().all(|r| r.is_ok()),
            "all concurrent puts should succeed: {results:?}"
        );

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, None)
            .await
            .expect("list versions");
        assert!(markers.is_empty());
        assert_eq!(versions.len(), 10, "each put creates one version");
        assert_eq!(
            versions.iter().filter(|m| m.is_latest).count(),
            1,
            "exactly one latest"
        );

        repo.get_object("b", "k").await.expect("get latest");
    }
}
