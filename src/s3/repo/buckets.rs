use redb::{ReadableDatabase, ReadableTable};
use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::instrument;

use super::{Repository, StoreResultExt};
use crate::storage::{BUCKETS, BucketRow, OBJECTS, decode, encode};

impl Repository {
    pub(crate) async fn get_bucket_versioning(&self, bucket: &str) -> S3Result<Option<String>> {
        let model = self.get_bucket(bucket).await?;
        Ok(model.versioning_status)
    }

    pub(crate) async fn put_bucket_versioning(
        &self,
        bucket: &str,
        status: Option<String>,
    ) -> S3Result<()> {
        let bucket = bucket.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BUCKETS).store()?;
                    let Some(mut row) = table
                        .get(bucket.as_str())
                        .store()?
                        .map(|value| decode::<BucketRow>(value.value()))
                        .transpose()
                        .store()?
                    else {
                        return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
                    };
                    row.versioning_status = status;
                    let bytes = encode(&row).store()?;
                    table.insert(bucket.as_str(), bytes.as_slice()).store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn create_bucket(&self, name: String, region: Option<String>) -> S3Result<()> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BUCKETS).store()?;
                    let exists = table.get(name.as_str()).store()?.is_some();
                    if exists {
                        return Err(S3Error::new(S3ErrorCode::BucketAlreadyExists));
                    }

                    let row = BucketRow {
                        id: name.clone(),
                        region,
                        created_at: chrono::Local::now().to_utc(),
                        versioning_status: None,
                    };
                    let bytes = encode(&row).store()?;
                    table.insert(name.as_str(), bytes.as_slice()).store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn list_buckets(&self) -> S3Result<Vec<BucketRow>> {
        self.store
            .run(|db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(BUCKETS).store()?;

                table
                    .iter()
                    .store()?
                    .map(|entry| {
                        let (_, value) = entry.store()?;
                        decode(value.value()).store()
                    })
                    .collect()
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn delete_bucket(&self, name: &str) -> S3Result<()> {
        let name = name.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut table = txn.open_table(BUCKETS).store()?;
                    table.remove(name.as_str()).store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn get_bucket_object_count(&self, name: &str) -> S3Result<u64> {
        let name = name.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(OBJECTS).store()?;
                let mut count = 0_u64;

                for entry in table.range((name.as_str(), "", "")..).store()? {
                    let (key, _) = entry.store()?;
                    if key.value().0 != name {
                        break;
                    }
                    count += 1;
                }

                Ok(count)
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn bucket_exists(&self, name: &str) -> S3Result<bool> {
        let name = name.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(BUCKETS).store()?;

                Ok(table.get(name.as_str()).store()?.is_some())
            })
            .await
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn get_bucket(&self, name: &str) -> S3Result<BucketRow> {
        self.inject(crate::storage::Fault::BucketRead)?;
        let name = name.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let table = txn.open_table(BUCKETS).store()?;
                let value = table
                    .get(name.as_str())
                    .store()?
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?;

                decode(value.value()).store()
            })
            .await
    }
}
