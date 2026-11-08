use std::{collections::HashSet, ops::Bound};

use chrono::{DateTime, Utc};
use redb::{ReadableDatabase, ReadableTable, WriteTransaction};
use s3s::{S3Error, S3ErrorCode, S3Result};
use tracing::instrument;

use super::{ObjectWrite, PutCondition, Repository, StoreResultExt};
use crate::s3::objects::MAX_KEYS_CEILING;
use crate::storage::{
    BUCKETS, BucketRow, Fault, LATEST, OBJECTS, ObjectRow, StoreError, VERSIONS, decode, encode,
    inverted_timestamp,
};

type ObjectKey = (&'static str, &'static str, &'static str);
type ObjectValue = &'static [u8];
type LatestKey = (&'static str, &'static str);
type LatestValue = &'static str;

/// One row of a `ListObjects` or `ListObjectsV2` page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectListRow {
    /// The key. It is also the `next_marker` cursor. It is also the input to
    /// the handler's `CommonPrefix` rollup.
    pub id: String,
    pub size: u32,
    pub last_modified: DateTime<Utc>,
}

/// One row of a `ListObjectVersions` page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectVersionRow {
    /// The key. It is also the `next_key_marker` cursor. It is also the
    /// `CommonPrefix` input.
    pub id: String,
    /// Also the `next_version_id_marker` cursor.
    pub version_id: String,
    pub is_latest: bool,
    /// Splits the page into versions and delete markers.
    pub is_delete_marker: bool,
    pub size: u32,
    pub last_modified: DateTime<Utc>,
    /// Rendered as the version's `e_tag`.
    pub etag: Option<String>,
}

fn bucket_row<T>(table: &T, name: &str) -> Result<Option<BucketRow>, StoreError>
where
    T: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(name)? else {
        return Ok(None);
    };

    Ok(Some(decode(value.value())?))
}

fn object_row<T>(
    table: &T,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<ObjectRow>, StoreError>
where
    T: ReadableTable<ObjectKey, ObjectValue>,
{
    let Some(value) = table.get((bucket, key, version_id))? else {
        return Ok(None);
    };

    Ok(Some(decode(value.value())?))
}

fn latest_id<T>(table: &T, bucket: &str, key: &str) -> Result<Option<String>, StoreError>
where
    T: ReadableTable<LatestKey, LatestValue>,
{
    Ok(table
        .get((bucket, key))?
        .map(|value| value.value().to_owned()))
}

fn latest_model<T, U>(
    latest: &T,
    objects: &U,
    bucket: &str,
    key: &str,
) -> Result<Option<ObjectRow>, StoreError>
where
    T: ReadableTable<LatestKey, LatestValue>,
    U: ReadableTable<ObjectKey, ObjectValue>,
{
    let Some(version_id) = latest_id(latest, bucket, key)? else {
        return Ok(None);
    };

    object_row(objects, bucket, key, &version_id)
}

fn version_key(row: &ObjectRow) -> (&str, &str, u64, &str) {
    (
        &row.bucket_id,
        &row.id,
        inverted_timestamp(row.last_modified),
        &row.version_id,
    )
}

fn set_latest(
    txn: &WriteTransaction,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<(), StoreError> {
    let mut table = txn.open_table(LATEST)?;
    table.insert((bucket, key), version_id)?;

    Ok(())
}

fn clear_latest(txn: &WriteTransaction, bucket: &str, key: &str) -> Result<(), StoreError> {
    let mut table = txn.open_table(LATEST)?;
    table.remove((bucket, key))?;

    Ok(())
}

/// Write a row, its version index entry, and the latest pointer.
fn insert_object_row(txn: &WriteTransaction, row: &ObjectRow) -> Result<(), StoreError> {
    {
        let mut objects = txn.open_table(OBJECTS)?;
        let bytes = encode(row)?;
        objects.insert(
            (
                row.bucket_id.as_str(),
                row.id.as_str(),
                row.version_id.as_str(),
            ),
            bytes.as_slice(),
        )?;
    }
    {
        let mut versions = txn.open_table(VERSIONS)?;
        versions.insert(version_key(row), &())?;
    }
    set_latest(txn, &row.bucket_id, &row.id, &row.version_id)?;

    Ok(())
}

/// Replace an existing version row and keep its index entry in step.
fn replace_object_row(txn: &WriteTransaction, row: &ObjectRow) -> Result<(), StoreError> {
    {
        let objects = txn.open_table(OBJECTS)?;
        if let Some(old) = object_row(&objects, &row.bucket_id, &row.id, &row.version_id)? {
            let mut versions = txn.open_table(VERSIONS)?;
            versions.remove(version_key(&old))?;
        }
    }
    insert_object_row(txn, row)
}

/// Delete one row and its version index entry.
fn delete_object_row(
    txn: &WriteTransaction,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<Option<ObjectRow>, StoreError> {
    let old = {
        let objects = txn.open_table(OBJECTS)?;
        object_row(&objects, bucket, key, version_id)?
    };

    if let Some(old) = &old {
        {
            let mut objects = txn.open_table(OBJECTS)?;
            objects.remove((bucket, key, version_id))?;
        }
        {
            let mut versions = txn.open_table(VERSIONS)?;
            versions.remove(version_key(old))?;
        }
    }

    Ok(old)
}

/// Set `is_latest = false` on the current latest row, when one exists.
fn demote_latest(txn: &WriteTransaction, bucket: &str, key: &str) -> Result<(), StoreError> {
    let mut row = {
        let latest = txn.open_table(LATEST)?;
        let objects = txn.open_table(OBJECTS)?;
        latest_model(&latest, &objects, bucket, key)?
    };

    if let Some(row) = row.as_mut()
        && row.is_latest
    {
        row.is_latest = false;
        let bytes = encode(row)?;
        let mut objects = txn.open_table(OBJECTS)?;
        objects.insert(
            (
                row.bucket_id.as_str(),
                row.id.as_str(),
                row.version_id.as_str(),
            ),
            bytes.as_slice(),
        )?;
    }

    Ok(())
}

/// Promote the newest remaining version of a key, or clear the pointer.
fn promote_newest(txn: &WriteTransaction, bucket: &str, key: &str) -> Result<(), StoreError> {
    let next = {
        let versions = txn.open_table(VERSIONS)?;
        match versions.range((bucket, key, 0_u64, "")..)?.next() {
            Some(entry) => {
                let (entry_key, _) = entry?;
                let (entry_bucket, entry_key, _, version_id) = entry_key.value();
                if entry_bucket == bucket && entry_key == key {
                    Some(version_id.to_owned())
                } else {
                    None
                }
            }
            None => None,
        }
    };

    let Some(version_id) = next else {
        clear_latest(txn, bucket, key)?;
        return Ok(());
    };

    {
        let mut row = {
            let objects = txn.open_table(OBJECTS)?;
            object_row(&objects, bucket, key, &version_id)?
        };
        if let Some(row) = row.as_mut() {
            row.is_latest = true;
            let bytes = encode(row)?;
            let mut objects = txn.open_table(OBJECTS)?;
            objects.insert(
                (
                    row.bucket_id.as_str(),
                    row.id.as_str(),
                    row.version_id.as_str(),
                ),
                bytes.as_slice(),
            )?;
        }
    }
    set_latest(txn, bucket, key, &version_id)?;

    Ok(())
}

impl Repository {
    async fn get_latest_model(&self, bucket: &str, key: &str) -> S3Result<Option<ObjectRow>> {
        self.inject(Fault::ObjectRead)?;
        let bucket = bucket.to_owned();
        let key = key.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let latest = txn.open_table(LATEST).store()?;
                let objects = txn.open_table(OBJECTS).store()?;

                latest_model(&latest, &objects, &bucket, &key).store()
            })
            .await
    }

    pub(crate) async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> S3Result<ObjectRow> {
        self.inject(Fault::ObjectRead)?;
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let objects = txn.open_table(OBJECTS).store()?;

                object_row(&objects, &bucket, &key, &version_id)
                    .store()?
                    .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))
            })
            .await
    }

    /// Atomic compare-and-swap object write.
    ///
    /// The condition check and the demote-plus-insert run in one redb write
    /// transaction. Concurrent writers serialize on that transaction. They do
    /// not interleave into zero or two `is_latest` rows. A crash between
    /// demote and insert cannot orphan the key.
    #[instrument(skip(self, data, condition), level = "debug", err)]
    pub(crate) async fn cas_put_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;

                let result = cas_put_object_txn(&txn, &bucket, &key, data, condition);
                match result {
                    Ok(version_id) => {
                        txn.commit().store()?;
                        Ok(version_id)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    /// Atomic publish. Record the new blob row, the object row, and the
    /// resolved publish intent in one transaction. The intent is recorded only
    /// when `publish_id` is set.
    #[instrument(skip(self, data, condition), level = "debug", err)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn publish_object_with_new_blob(
        &self,
        blob_id: String,
        publish_id: Option<&str>,
        bucket: String,
        key: String,
        data: ObjectWrite,
        condition: PutCondition,
    ) -> S3Result<String> {
        if publish_id.is_some() {
            self.inject(Fault::PublishIntentUpdate)?;
        }
        let publish_id = publish_id.map(str::to_owned);

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let blob_size = data.size;

                let result = (|| {
                    super::blobs::insert_blob_in_txn(&txn, &blob_id, blob_size)?;
                    let version_id = cas_put_object_txn(&txn, &bucket, &key, data, condition)?;

                    if let Some(publish_id) = &publish_id {
                        super::blobs::resolve_publish_intent_in_txn(
                            &txn,
                            publish_id,
                            blob_id.clone(),
                        )?;
                    }

                    Ok(version_id)
                })();

                match result {
                    Ok(version_id) => {
                        txn.commit().store()?;
                        Ok(version_id)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    pub(crate) async fn set_object_tags_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        tags: serde_json::Value,
    ) -> S3Result<()> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.map(str::to_owned);

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                {
                    let mut objects = txn.open_table(OBJECTS).store()?;
                    let target = match &version_id {
                        Some(version_id) => {
                            object_row(&objects, &bucket, &key, version_id).store()?
                        }
                        None => {
                            let latest = txn.open_table(LATEST).store()?;
                            latest_model(&latest, &objects, &bucket, &key).store()?
                        }
                    };
                    let Some(mut row) = target else {
                        return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                    };
                    row.tags = tags;
                    let bytes = encode(&row).store()?;
                    objects
                        .insert(
                            (
                                row.bucket_id.as_str(),
                                row.id.as_str(),
                                row.version_id.as_str(),
                            ),
                            bytes.as_slice(),
                        )
                        .store()?;
                }
                txn.commit().store()?;

                Ok(())
            })
            .await
    }

    /// Version id of the current latest entry for a key, if any.
    pub(crate) async fn latest_version_id(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<String>> {
        Ok(self
            .get_latest_model(bucket, key)
            .await?
            .map(|row| row.version_id))
    }

    /// Latest version row, even if it is a delete marker. Returns `None` when
    /// the key has no versions.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn get_latest_raw(
        &self,
        bucket: &str,
        key: &str,
    ) -> S3Result<Option<ObjectRow>> {
        self.get_latest_model(bucket, key).await
    }

    #[instrument(skip(self, data), level = "debug", err)]
    pub(crate) async fn upsert_object(
        &self,
        bucket: String,
        key: String,
        data: ObjectWrite,
    ) -> S3Result<String> {
        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let versioning = {
                    let buckets = txn.open_table(BUCKETS).store()?;
                    bucket_row(&buckets, &bucket)
                        .store()?
                        .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?
                        .versioning_status
                };
                let now = chrono::Local::now().to_utc();
                let result = put_object_internal(&txn, &bucket, &key, data, versioning, now);

                match result {
                    Ok(version_id) => {
                        txn.commit().store()?;
                        Ok(version_id)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    #[allow(dead_code)]
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn object_exists(&self, bucket: &str, key: &str) -> S3Result<bool> {
        let row = self.get_latest_model(bucket, key).await?;
        Ok(row.is_some_and(|value| !value.is_delete_marker))
    }

    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn get_object(&self, bucket: &str, key: &str) -> S3Result<ObjectRow> {
        let row = self
            .get_latest_model(bucket, key)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;
        if row.is_delete_marker {
            return Err(S3Error::new(S3ErrorCode::NoSuchKey));
        }

        Ok(row)
    }

    /// Versioned delete.
    ///
    /// If `version_id` is Some, permanently delete that version. If None,
    /// create a delete marker when bucket versioning is enabled. Otherwise
    /// permanently delete the null version. Returns (`deleted_model`,
    /// `is_delete_marker_created`).
    ///
    /// Every change runs in one redb write transaction. "At most one row per
    /// key has `is_latest = true`" is upheld by holding that transaction
    /// across demote, insert, and promote. A concurrent transaction serializes
    /// behind this one. It never observes an intermediate state.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn delete_object_versioned(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> S3Result<(Option<ObjectRow>, bool)> {
        let bucket = bucket.to_owned();
        let key = key.to_owned();
        let version_id = version_id.map(str::to_owned);

        self.store
            .run(move |db| {
                let txn = db.begin_write().store()?;
                let result =
                    delete_object_versioned_txn(&txn, &bucket, &key, version_id.as_deref());

                match result {
                    Ok(output) => {
                        txn.commit().store()?;
                        Ok(output)
                    }
                    Err(err) => Err(err),
                }
            })
            .await
    }

    /// List the latest, non-delete-marker version of every key in a page.
    #[instrument(skip(self), level = "debug", err)]
    pub(crate) async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<String>,
        delimiter: Option<String>,
        marker: Option<String>,
        limit: u64,
    ) -> S3Result<Vec<ObjectListRow>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let bucket = bucket.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let latest = txn.open_table(LATEST).store()?;
                let objects = txn.open_table(OBJECTS).store()?;

                let start = match marker.as_deref() {
                    Some(marker) => Bound::Excluded((bucket.as_str(), marker)),
                    None => {
                        Bound::Included((bucket.as_str(), prefix.as_deref().unwrap_or_default()))
                    }
                };
                let upper = prefix.as_deref().map(crate::storage::prefix_upper);
                let end = match upper.as_deref() {
                    Some(upper) => Bound::Excluded((bucket.as_str(), upper)),
                    None => Bound::Unbounded,
                };

                let mut rows = Vec::new();
                let mut rolled: HashSet<String> = HashSet::new();

                for entry in latest.range((start, end)).store()? {
                    let (key, value) = entry.store()?;
                    let (entry_bucket, key) = key.value();
                    if entry_bucket != bucket {
                        break;
                    }
                    if let Some(prefix) = prefix.as_deref()
                        && !key.starts_with(prefix)
                    {
                        continue;
                    }

                    let version_id = value.value();
                    let Some(row) = object_row(&objects, &bucket, key, version_id).store()? else {
                        continue;
                    };
                    if row.is_delete_marker {
                        continue;
                    }

                    if let Some(delimiter) = delimiter.as_deref()
                        && let Some(common) =
                            common_prefix_for(key, prefix.as_deref().unwrap_or_default(), delimiter)
                        && !rolled.insert(common)
                    {
                        continue;
                    }

                    rows.push(ObjectListRow {
                        id: row.id,
                        size: row.size,
                        last_modified: row.last_modified,
                    });

                    if rows.len() as u64 == limit {
                        break;
                    }
                }

                Ok(rows)
            })
            .await
    }

    /// List one page of a bucket's object versions.
    ///
    /// Rows come back ordered `Id ASC, LastModified DESC, VersionId ASC`.
    /// That is the S3 version-listing order. The page is bounded by the
    /// caller's fetch cap.
    ///
    /// With only `key_marker`, the cursor is the key. With `key_marker` plus
    /// `version_id_marker`, the cursor is that exact row. Rows after it have a
    /// greater key or an older `last_modified`. An unknown `version_id_marker`
    /// degrades to the key-only boundary.
    ///
    /// Callers pass `max_keys + 1`. They use the extra row as the truncation
    /// sentinel.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<String>,
        _delimiter: Option<String>,
        key_marker: Option<String>,
        version_id_marker: Option<String>,
        max_keys: Option<i32>,
    ) -> S3Result<(Vec<ObjectVersionRow>, Vec<ObjectVersionRow>)> {
        let limit = usize::try_from(max_keys.unwrap_or(MAX_KEYS_CEILING))
            .map_err(|_| S3Error::new(S3ErrorCode::InvalidArgument))?;
        if limit == 0 {
            return Ok((Vec::new(), Vec::new()));
        }
        let bucket = bucket.to_owned();

        self.store
            .run(move |db| {
                let txn = db.begin_read().store()?;
                let objects = txn.open_table(OBJECTS).store()?;
                let versions = txn.open_table(VERSIONS).store()?;

                let cursor_at = match (key_marker.as_deref(), version_id_marker.as_deref()) {
                    (Some(key_marker), Some(version_id_marker)) => {
                        object_row(&objects, &bucket, key_marker, version_id_marker)
                            .store()?
                            .map(|row| (row.last_modified, row.version_id))
                    }
                    _ => None,
                };

                let start = key_marker
                    .as_deref()
                    .or(prefix.as_deref())
                    .unwrap_or_default();

                let upper = prefix.as_deref().map(crate::storage::prefix_upper);
                let end = match upper.as_deref() {
                    Some(upper) => Bound::Excluded((bucket.as_str(), upper, 0_u64, "")),
                    None => Bound::Unbounded,
                };

                let mut page = Vec::new();

                for entry in versions
                    .range((Bound::Included((bucket.as_str(), start, 0_u64, "")), end))
                    .store()?
                {
                    let (key, _) = entry.store()?;
                    let (entry_bucket, key, inverted, version_id) = key.value();
                    if entry_bucket != bucket {
                        break;
                    }
                    if let Some(prefix) = prefix.as_deref()
                        && !key.starts_with(prefix)
                    {
                        continue;
                    }

                    if let Some(key_marker) = key_marker.as_deref()
                        && key == key_marker
                    {
                        let Some((cursor_at, _)) = &cursor_at else {
                            continue;
                        };
                        if inverted <= inverted_timestamp(*cursor_at) {
                            continue;
                        }
                    }

                    let Some(row) = object_row(&objects, &bucket, key, version_id).store()? else {
                        continue;
                    };
                    page.push(ObjectVersionRow {
                        id: row.id,
                        version_id: row.version_id,
                        is_latest: row.is_latest,
                        is_delete_marker: row.is_delete_marker,
                        size: row.size,
                        last_modified: row.last_modified,
                        etag: row.etag,
                    });

                    if page.len() == limit {
                        break;
                    }
                }

                let mut versions = Vec::new();
                let mut delete_markers = Vec::new();
                for row in page {
                    if row.is_delete_marker {
                        delete_markers.push(row);
                    } else {
                        versions.push(row);
                    }
                }

                Ok((versions, delete_markers))
            })
            .await
    }
}

fn common_prefix_for(key: &str, prefix: &str, delimiter: &str) -> Option<String> {
    let without_prefix = key.strip_prefix(prefix).unwrap_or(key);

    without_prefix
        .split_once(delimiter)
        .map(|(sub_key, _)| format!("{prefix}{sub_key}{delimiter}"))
}

pub(super) fn cas_put_object_txn(
    txn: &WriteTransaction,
    bucket: &str,
    key: &str,
    data: ObjectWrite,
    condition: PutCondition,
) -> S3Result<String> {
    let versioning = {
        let buckets = txn.open_table(BUCKETS).store()?;
        bucket_row(&buckets, bucket)
            .store()?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?
            .versioning_status
    };

    let latest_opt = {
        let latest = txn.open_table(LATEST).store()?;
        let objects = txn.open_table(OBJECTS).store()?;
        latest_model(&latest, &objects, bucket, key).store()?
    };
    let exists_not_deleted = latest_opt.as_ref().is_some_and(|row| !row.is_delete_marker);
    let current_etag = if exists_not_deleted {
        latest_opt.as_ref().and_then(|row| row.etag.clone())
    } else {
        None
    };

    match &condition {
        PutCondition::IfMatch(expected) => {
            if !exists_not_deleted {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            if current_etag.as_deref() != Some(expected.as_str()) {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        }
        PutCondition::IfMatchAny => {
            if !exists_not_deleted {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
        }
        PutCondition::IfNoneMatchAny => {
            if exists_not_deleted {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        }
        PutCondition::IfNoneMatch(expected) => {
            if exists_not_deleted && current_etag.as_deref() == Some(expected.as_str()) {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        }
        PutCondition::None => {}
    }

    let now = chrono::Local::now().to_utc();
    put_object_internal(txn, bucket, key, data, versioning, now)
}

fn put_object_internal(
    txn: &WriteTransaction,
    bucket: &str,
    key: &str,
    data: ObjectWrite,
    versioning: Option<String>,
    now: DateTime<Utc>,
) -> S3Result<String> {
    let size = Repository::checked_size(data.size)?;
    let make_row = |version_id: String| ObjectRow {
        bucket_id: bucket.to_owned(),
        id: key.to_owned(),
        version_id,
        is_latest: true,
        is_delete_marker: false,
        size,
        last_modified: now,
        content_type: data.content_type.clone(),
        etag: data.etag.clone(),
        user_metadata: data.user_metadata.clone(),
        tags: data.tags.clone(),
        checksums: data.checksums.clone(),
        content: data.content.clone(),
    };

    match versioning.as_deref() {
        None => {
            let version_id = "null".to_owned();
            replace_object_row(txn, &make_row(version_id.clone())).store()?;
            Ok(version_id)
        }
        Some("Enabled") => {
            demote_latest(txn, bucket, key).store()?;
            let version_id = uuid::Uuid::new_v4().to_string();
            insert_object_row(txn, &make_row(version_id.clone())).store()?;
            Ok(version_id)
        }
        Some(_) => {
            demote_latest(txn, bucket, key).store()?;
            let version_id = "null".to_owned();
            replace_object_row(txn, &make_row(version_id.clone())).store()?;
            Ok(version_id)
        }
    }
}

fn delete_object_versioned_txn(
    txn: &WriteTransaction,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<(Option<ObjectRow>, bool)> {
    if let Some(version_id) = version_id {
        let model = delete_object_row(txn, bucket, key, version_id)
            .store()?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        if model.is_latest {
            promote_newest(txn, bucket, key).store()?;
        }

        return Ok((Some(model), false));
    }

    let status = {
        let buckets = txn.open_table(BUCKETS).store()?;
        bucket_row(&buckets, bucket)
            .store()?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchBucket))?
            .versioning_status
    };

    if status.is_none() {
        let model = delete_object_row(txn, bucket, key, "null").store()?;
        if model.is_some() {
            clear_latest(txn, bucket, key).store()?;
            return Ok((model, false));
        }

        // No such key; S3 delete is idempotent, return Ok with no model.
        return Ok((None, false));
    }

    demote_latest(txn, bucket, key).store()?;
    let version_id = uuid::Uuid::new_v4().to_string();
    let marker = ObjectRow {
        bucket_id: bucket.to_owned(),
        id: key.to_owned(),
        version_id: version_id.clone(),
        is_latest: true,
        is_delete_marker: true,
        size: 0,
        last_modified: chrono::Local::now().to_utc(),
        content_type: None,
        etag: None,
        user_metadata: serde_json::json!({}),
        tags: serde_json::json!([]),
        checksums: serde_json::json!({}),
        content: serde_json::json!({"item": []}),
    };
    insert_object_row(txn, &marker).store()?;

    Ok((Some(marker), true))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;
    use crate::storage::{BlobRow, PublishIntentRow, Store};

    async fn repo() -> Repository {
        let store = Store::in_memory().expect("store");
        Repository::init(store).await.expect("init")
    }

    fn write(content: serde_json::Value, etag: &str) -> ObjectWrite {
        ObjectWrite {
            size: 1,
            content_type: None,
            etag: Some(etag.into()),
            content,
            user_metadata: serde_json::json!({}),
            checksums: serde_json::json!({}),
            tags: serde_json::json!([]),
        }
    }

    fn content_for(blob_id: &str) -> serde_json::Value {
        serde_json::json!({"item": [{"id": blob_id, "offset": 0, "size": 1}]})
    }

    async fn versioned_bucket(repo: &Repository) {
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.put_bucket_versioning("b", Some("Enabled".into()))
            .await
            .expect("enable versioning");
    }

    async fn versioned_bucket_with_keys(repo: &Repository, keys: &[&str]) {
        versioned_bucket(repo).await;
        for key in keys {
            repo.cas_put_object(
                "b".into(),
                (*key).to_string(),
                write(serde_json::json!({"item": []}), &format!("etag-{key}")),
                PutCondition::None,
            )
            .await
            .unwrap_or_else(|e| panic!("put {key}: {e:?}"));
        }
    }

    fn seed_row(
        repo: &Repository,
        key: &str,
        version_id: &str,
        at: &str,
        is_latest: bool,
        is_delete_marker: bool,
    ) {
        let last_modified = chrono::DateTime::parse_from_rfc3339(at)
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        repo.store.seed_object(&ObjectRow {
            bucket_id: "b".into(),
            id: key.into(),
            version_id: version_id.into(),
            is_latest,
            is_delete_marker,
            size: 7,
            last_modified,
            content_type: Some("text/plain".into()),
            etag: Some(format!("etag-{version_id}")),
            user_metadata: serde_json::json!({"x-amz-meta-a": "1"}),
            tags: serde_json::json!([{"key": "t", "value": "v"}]),
            checksums: serde_json::json!({"sha256": "abc"}),
            content: serde_json::json!({"item": [{"id": "blob-1", "size": 7}]}),
        });
    }

    fn seed_version(repo: &Repository, version_id: &str, at: &str, is_latest: bool) {
        let last_modified = chrono::DateTime::parse_from_rfc3339(at)
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        repo.store.seed_object(&ObjectRow {
            bucket_id: "b".into(),
            id: "k".into(),
            version_id: version_id.into(),
            is_latest,
            is_delete_marker: false,
            size: 1,
            last_modified,
            content_type: None,
            etag: Some(format!("etag-{version_id}")),
            user_metadata: serde_json::json!({}),
            tags: serde_json::json!([]),
            checksums: serde_json::json!({}),
            content: serde_json::json!({"item": []}),
        });
    }

    fn rows_for_key(repo: &Repository) -> Vec<ObjectRow> {
        repo.store
            .all_objects("b")
            .into_iter()
            .filter(|row| row.id == "k")
            .collect()
    }

    fn blob_rows(repo: &Repository) -> Vec<BlobRow> {
        repo.store.all_blobs()
    }

    #[tokio::test]
    async fn list_objects_returns_prefix_and_delimiter_shapes() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        seed_row(&repo, "a", "v1", "2024-01-01T00:00:00Z", true, false);
        seed_row(&repo, "a/x", "v1", "2024-01-02T00:00:00Z", true, false);
        seed_row(&repo, "a/y", "v1", "2024-01-03T00:00:00Z", true, false);
        seed_row(&repo, "b", "v1", "2024-01-04T00:00:00Z", true, false);

        let rows = repo
            .list_objects("b", None, None, None, 1000)
            .await
            .expect("list");
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a", "a/x", "a/y", "b"]
        );
        assert!(rows.iter().all(|r| r.size == 7));

        let rows = repo
            .list_objects("b", Some("a/".into()), None, None, 1000)
            .await
            .expect("list prefix");
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a/x", "a/y"]
        );

        let rows = repo
            .list_objects("b", None, Some("/".into()), None, 1000)
            .await
            .expect("list delimiter");
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a", "a/x", "b"],
            "the delimiter keeps the first key per common prefix"
        );

        let rows = repo
            .list_objects("b", None, None, Some("a".into()), 1000)
            .await
            .expect("list marker");
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a/x", "a/y", "b"],
            "the marker skips keys at or before itself"
        );
    }

    #[tokio::test]
    async fn list_objects_bounds_the_page() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        for key in ["a", "b", "c"] {
            seed_row(&repo, key, "v1", "2024-01-01T00:00:00Z", true, false);
        }

        let rows = repo
            .list_objects("b", None, None, None, 2)
            .await
            .expect("page");
        assert_eq!(
            rows.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[tokio::test]
    async fn list_object_versions_bounds_rows_to_limit() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "a/3", "b/1", "b/2"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(2))
            .await
            .expect("page 1");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/1", "a/2"]
        );
    }

    #[tokio::test]
    async fn version_page_two_starts_after_both_markers() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "a/3", "b/1", "b/2"]).await;

        let (versions, page1_markers) = repo
            .list_object_versions("b", None, None, None, None, Some(2))
            .await
            .expect("page 1");
        assert!(page1_markers.is_empty());
        let last = versions.last().expect("page 1 must be non-empty");

        let (versions, markers) = repo
            .list_object_versions(
                "b",
                None,
                None,
                Some(last.id.clone()),
                Some(last.version_id.clone()),
                Some(2),
            )
            .await
            .expect("page 2");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/3", "b/1"],
            "continuation markers must resume at the next row, not repeat or skip"
        );
    }

    #[tokio::test]
    async fn version_key_marker_alone_skips_the_whole_marker_key() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "a/2", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, Some("a/1".into()), None, Some(10))
            .await
            .expect("list");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["a/2", "b/1"]
        );
    }

    #[tokio::test]
    async fn unknown_version_marker_degrades_to_key_boundary() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions(
                "b",
                None,
                None,
                Some("a/1".into()),
                Some("no-such-version".into()),
                Some(10),
            )
            .await
            .expect("list");
        assert!(markers.is_empty());
        assert_eq!(
            versions.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            ["b/1"],
            "an unresolvable version marker must not resurrect the marked key"
        );
    }

    #[tokio::test]
    async fn version_pages_of_zero_limit_return_nothing() {
        let repo = repo().await;
        versioned_bucket_with_keys(&repo, &["a/1", "b/1"]).await;

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(0))
            .await
            .expect("list");
        assert!(versions.is_empty());
        assert!(markers.is_empty());
    }

    #[tokio::test]
    async fn delete_markers_stay_out_of_a_plain_listing() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_row(&repo, "k", "v1", "2024-01-01T00:00:00Z", false, false);
        seed_row(&repo, "k", "marker-1", "2024-01-02T00:00:00Z", true, true);

        let rows = repo
            .list_objects("b", None, None, None, 1000)
            .await
            .expect("list");
        assert!(rows.is_empty(), "a delete marker is not an object");

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(10))
            .await
            .expect("list versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(markers.len(), 1, "the delete marker must still be listed");

        let marker = &markers[0];
        assert!(marker.is_delete_marker);
        assert!(marker.is_latest);
        assert_eq!(marker.version_id, "marker-1");
        assert_eq!(marker.size, 7);
    }

    #[tokio::test]
    async fn version_listing_keeps_newest_first_order_per_key() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_row(&repo, "k", "v1", "2024-01-01T00:00:00Z", false, false);
        seed_row(&repo, "k", "v3", "2024-01-03T00:00:00Z", true, false);
        seed_row(&repo, "k", "v2", "2024-01-02T00:00:00Z", false, false);
        seed_row(&repo, "other", "o1", "2024-01-04T00:00:00Z", true, false);

        let (versions, markers) = repo
            .list_object_versions("b", None, None, None, None, Some(10))
            .await
            .expect("list versions");

        assert!(markers.is_empty());
        assert_eq!(
            versions
                .iter()
                .map(|m| (m.id.as_str(), m.version_id.as_str()))
                .collect::<Vec<_>>(),
            [("k", "v3"), ("k", "v2"), ("k", "v1"), ("other", "o1")],
            "key ascending, newest version first within a key"
        );
        assert_eq!(
            versions
                .iter()
                .filter(|m| m.is_latest)
                .map(|m| m.version_id.as_str())
                .collect::<Vec<_>>(),
            ["v3", "o1"]
        );
    }

    #[tokio::test]
    async fn deleting_latest_version_promotes_newest_predecessor() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", false);
        seed_version(&repo, "v2", "2024-01-02T00:00:00Z", false);
        seed_version(&repo, "v3", "2024-01-03T00:00:00Z", true);

        let (deleted, marker) = repo
            .delete_object_versioned("b", "k", Some("v3"))
            .await
            .expect("delete latest");
        assert_eq!(
            deleted.map(|m| m.version_id).as_deref(),
            Some("v3"),
            "permanent delete returns the removed row"
        );
        assert!(!marker, "permanent delete creates no delete marker");

        let rows = rows_for_key(&repo);
        let latest: Vec<_> = rows.iter().filter(|m| m.is_latest).collect();
        assert_eq!(
            latest
                .iter()
                .map(|m| m.version_id.as_str())
                .collect::<Vec<_>>(),
            ["v2"],
            "the newest remaining version must be promoted"
        );

        let got = repo
            .get_object("b", "k")
            .await
            .expect("get promoted object");
        assert_eq!(got.version_id, "v2");
        assert_eq!(got.etag.as_deref(), Some("etag-v2"));
    }

    #[tokio::test]
    async fn deleting_the_last_remaining_version_leaves_no_latest() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", true);

        repo.delete_object_versioned("b", "k", Some("v1"))
            .await
            .expect("delete only version");

        assert!(
            rows_for_key(&repo).is_empty(),
            "the only version row is gone"
        );
    }

    #[tokio::test]
    async fn deleting_the_newest_version_leaves_object_readable() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", false);
        seed_version(&repo, "v2", "2024-01-02T00:00:00Z", true);

        let (marker_model, is_marker) = repo
            .delete_object_versioned("b", "k", None)
            .await
            .expect("delete without version id");
        assert!(is_marker, "versioned bucket must create a delete marker");
        let marker_model = marker_model.expect("delete marker model");
        assert!(marker_model.is_delete_marker && marker_model.is_latest);

        let latest = rows_for_key(&repo)
            .into_iter()
            .filter(|m| m.is_latest)
            .count();
        assert_eq!(latest, 1, "the delete marker is the single latest row");

        repo.delete_object_versioned("b", "k", Some(&marker_model.version_id))
            .await
            .expect("delete the marker");

        let got = repo.get_object("b", "k").await.expect("object is readable");
        assert_eq!(got.version_id, "v2");
        assert_eq!(
            rows_for_key(&repo).iter().filter(|m| m.is_latest).count(),
            1,
            "promotion after marker removal leaves exactly one latest"
        );
    }

    #[tokio::test]
    async fn unknown_version_delete_is_a_no_op_inside_the_transaction() {
        let repo = repo().await;
        versioned_bucket(&repo).await;
        seed_version(&repo, "v1", "2024-01-01T00:00:00Z", true);

        let err = repo
            .delete_object_versioned("b", "k", Some("nope"))
            .await
            .expect_err("unknown version must fail");
        assert!(
            format!("{err:?}").contains("NoSuchKey"),
            "expected NoSuchKey, got {err:?}"
        );

        let rows = rows_for_key(&repo);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_latest, "v1 stays latest after a failed delete");
    }

    #[tokio::test]
    async fn publish_records_the_blob_row_and_the_object_row_together() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let version_id = repo
            .publish_object_with_new_blob(
                "blob-1".into(),
                None,
                "b".into(),
                "k".into(),
                write(content_for("blob-1"), "e1"),
                PutCondition::None,
            )
            .await
            .expect("publish");

        assert_eq!(version_id, "null");
        let blobs = blob_rows(&repo);
        assert_eq!(blobs.len(), 1, "the blob row must exist");
        assert_eq!(blobs[0].id, "blob-1");
        assert_eq!(blobs[0].refs, 1);

        let object = repo.get_object("b", "k").await.expect("published object");
        assert_eq!(object.content, content_for("blob-1"));
    }

    #[tokio::test]
    async fn publish_resolves_the_publish_intent_with_the_blob_id() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.record_publish_intent("pid".into(), "b".into(), "k".into(), 1)
            .await
            .expect("record intent");

        repo.publish_object_with_new_blob(
            "blob-1".into(),
            Some("pid"),
            "b".into(),
            "k".into(),
            write(content_for("blob-1"), "e1"),
            PutCondition::None,
        )
        .await
        .expect("publish");

        let intents = repo.store.all_publish_intents();
        let intent = intents
            .iter()
            .find(|row| row.publish_id == "pid")
            .expect("intent row");
        assert!(
            intent.resolved_at.is_some(),
            "the publish must resolve its intent: {intent:?}"
        );
        assert_eq!(intent.blob_id.as_deref(), Some("blob-1"));
    }

    #[tokio::test]
    async fn a_missing_publish_intent_rolls_back_the_publish() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");

        let err = repo
            .publish_object_with_new_blob(
                "blob-1".into(),
                Some("missing"),
                "b".into(),
                "k".into(),
                write(content_for("blob-1"), "e1"),
                PutCondition::None,
            )
            .await
            .expect_err("a missing intent must fail the publish");
        assert_eq!(*err.code(), S3ErrorCode::InternalError);

        assert!(
            blob_rows(&repo).is_empty(),
            "the rolled-back publish must not leave a blob row behind"
        );
        assert!(
            !repo.object_exists("b", "k").await.expect("exists"),
            "the rolled-back publish must not leave an object row behind"
        );
    }

    #[tokio::test]
    async fn failed_publish_leaves_no_orphaned_blob_row() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.cas_put_object(
            "b".into(),
            "k".into(),
            write(serde_json::json!({"item": []}), "e1"),
            PutCondition::None,
        )
        .await
        .expect("seed object");

        let err = repo
            .publish_object_with_new_blob(
                "blob-orphan".into(),
                None,
                "b".into(),
                "k".into(),
                write(content_for("blob-orphan"), "e2"),
                PutCondition::IfNoneMatchAny,
            )
            .await
            .expect_err("precondition must fail");
        assert!(
            format!("{err:?}").contains("PreconditionFailed"),
            "expected PreconditionFailed, got {err:?}"
        );

        assert!(
            blob_rows(&repo).is_empty(),
            "a failed publish must not leave a blob row behind"
        );
        let object = repo.get_object("b", "k").await.expect("object unchanged");
        assert_eq!(object.etag.as_deref(), Some("e1"));
    }

    #[tokio::test]
    async fn publish_into_a_missing_bucket_leaves_no_blob_row() {
        let repo = repo().await;

        let err = repo
            .publish_object_with_new_blob(
                "blob-orphan".into(),
                None,
                "no-such-bucket".into(),
                "k".into(),
                write(content_for("blob-orphan"), "e1"),
                PutCondition::None,
            )
            .await
            .expect_err("missing bucket must fail");
        assert!(
            format!("{err:?}").contains("NoSuchBucket"),
            "expected NoSuchBucket, got {err:?}"
        );

        assert!(
            blob_rows(&repo).is_empty(),
            "the rolled-back publish must not leave a blob row behind"
        );
    }

    #[tokio::test]
    async fn blob_id_is_rejected_before_the_object_is_written() {
        let repo = repo().await;
        repo.create_bucket("b".into(), None)
            .await
            .expect("create bucket");
        repo.register_new_blob("blob-1".into(), 1)
            .await
            .expect("register");

        repo.publish_object_with_new_blob(
            "blob-1".into(),
            None,
            "b".into(),
            "k".into(),
            write(content_for("blob-1"), "e1"),
            PutCondition::None,
        )
        .await
        .expect_err("duplicate blob id must fail");

        let blobs = blob_rows(&repo);
        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].refs, 1, "the existing row is untouched");
        assert!(
            !repo.object_exists("b", "k").await.expect("exists"),
            "no object row may survive a failed publish"
        );
    }

    #[tokio::test]
    async fn pending_publish_intents_are_bounded_and_ordered_by_age() {
        let repo = repo().await;
        let now = chrono::Local::now().to_utc();
        for (index, age) in [2_i64, 4, 3].into_iter().enumerate() {
            repo.store.seed_publish_intent(&PublishIntentRow {
                publish_id: format!("id-{index}"),
                bucket_id: "b".into(),
                object_id: "k".into(),
                size: 1,
                blob_id: None,
                created_at: now - chrono::Duration::hours(age),
                resolved_at: None,
            });
        }

        let rows = repo.pending_publish_intents(now, 2).await.expect("pending");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| row.publish_id.as_str())
                .collect::<Vec<_>>(),
            ["id-1", "id-2"],
            "the oldest pending intents come first"
        );
    }
}
