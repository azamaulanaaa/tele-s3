use std::{ops::Deref, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use s3s::{
    Body, S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
        DeletedObject, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput,
        ListObjectsInput, ListObjectsOutput, ListObjectsV2Input, ListObjectsV2Output, Object,
        PutObjectInput, PutObjectOutput, Timestamp,
    },
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
    sea_query::OnConflict,
};

mod entity;

pub struct MetadataStorage {
    inner: DatabaseConnection,
}

impl MetadataStorage {
    pub async fn init(connection: DatabaseConnection) -> anyhow::Result<Self> {
        Self::sync_table(&connection).await?;

        Ok(Self { inner: connection })
    }

    async fn sync_table(connection: &DatabaseConnection) -> anyhow::Result<(), DbErr> {
        connection
            .get_schema_registry(concat!(module_path!(), "::entity"))
            .sync(connection)
            .await?;

        Ok(())
    }
}

impl Deref for MetadataStorage {
    type Target = DatabaseConnection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl S3 for MetadataStorage {
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let body = {
            let body_stream = req
                .input
                .body
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            let body: Vec<Bytes> = body_stream
                .try_collect()
                .await
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
            let body = body.into_iter().flatten().collect::<Vec<u8>>();

            body
        };

        let content: serde_json::Value =
            serde_json::from_slice(&body).map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest))?;

        let etag = format!("\"{:x}\"", md5::compute(&body));

        let active_model = entity::metadata::ActiveModel {
            bucket: Set(req.input.bucket),
            key: Set(req.input.key),
            size: Set(body.len() as u32),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(req.input.content_type.map(|v| v.to_string())),
            etag: Set(Some(etag.clone())),
            content: Set(content),
        };

        entity::metadata::Entity::insert(active_model)
            .on_conflict(
                OnConflict::columns([
                    entity::metadata::Column::Bucket,
                    entity::metadata::Column::Key,
                ])
                .update_columns([
                    entity::metadata::Column::Size,
                    entity::metadata::Column::LastModified,
                    entity::metadata::Column::ContentType,
                    entity::metadata::Column::Etag,
                    entity::metadata::Column::Content,
                ])
                .to_owned(),
            )
            .exec(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let res = S3Response::new(PutObjectOutput {
            e_tag: Some(etag),
            ..Default::default()
        });

        Ok(res)
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = entity::metadata::Entity::find_by_id((req.input.bucket, req.input.key))
            .one(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let body_str = model.content.to_string();
        let body_bytes = body_str.into_bytes();

        let last_modified = {
            let last_modified: SystemTime = model.last_modified.into();
            let last_modified = Timestamp::from(last_modified);

            Some(last_modified)
        };

        let res = S3Response::new(GetObjectOutput {
            content_type: model.content_type.map(|v| v.parse().ok()).flatten(),
            last_modified,
            e_tag: model.etag,
            body: Some(Body::from(body_bytes).into()),
            ..Default::default()
        });

        Ok(res)
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let model = entity::metadata::Entity::find_by_id((req.input.bucket, req.input.key))
            .select_only()
            .columns([
                entity::metadata::Column::Size,
                entity::metadata::Column::ContentType,
                entity::metadata::Column::Etag,
                entity::metadata::Column::LastModified,
            ])
            .one(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let last_modified = {
            let last_modified: SystemTime = model.last_modified.into();
            let last_modified = Timestamp::from(last_modified);

            Some(last_modified)
        };

        let res = S3Response::new(HeadObjectOutput {
            content_type: model.content_type.map(|v| v.parse().ok()).flatten(),
            last_modified,
            e_tag: model.etag,
            ..Default::default()
        });

        Ok(res)
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        entity::metadata::Entity::delete_by_id((req.input.bucket, req.input.key))
            .exec(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let res = S3Response::new(DeleteObjectOutput::default());

        Ok(res)
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let keys: Vec<String> = req
            .input
            .delete
            .objects
            .iter()
            .map(|obj| obj.key.clone())
            .collect();

        entity::metadata::Entity::delete_many()
            .filter(entity::metadata::Column::Bucket.eq(req.input.bucket.clone()))
            .filter(entity::metadata::Column::Key.is_in(keys.clone()))
            .exec(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let quiet = req.input.delete.quiet.unwrap_or(false);

        let deleted = if quiet {
            None
        } else {
            let deleted_objects: Vec<DeletedObject> = keys
                .into_iter()
                .map(|key| DeletedObject {
                    key: Some(key),
                    ..Default::default()
                })
                .collect();
            Some(deleted_objects)
        };

        let res = S3Response::new(DeleteObjectsOutput {
            deleted,
            errors: None,
            ..Default::default()
        });

        Ok(res)
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let mut query = entity::metadata::Entity::find()
            .filter(entity::metadata::Column::Bucket.eq(req.input.bucket.clone()))
            .order_by_asc(entity::metadata::Column::Key);

        if let Some(prefix) = &req.input.prefix {
            query = query.filter(entity::metadata::Column::Key.starts_with(prefix.clone()));
        }

        if let Some(marker) = &req.input.marker {
            query = query.filter(entity::metadata::Column::Key.gt(marker.clone()));
        }

        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let items = query
            .limit(Some(limit))
            .select_only()
            .columns([
                entity::metadata::Column::Key,
                entity::metadata::Column::Size,
                entity::metadata::Column::LastModified,
                entity::metadata::Column::Etag,
            ])
            .all(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let contents: Vec<Object> = items
            .iter()
            .map(|model| {
                let last_modified = {
                    let last_modified: SystemTime = model.last_modified.into();
                    Some(Timestamp::from(last_modified))
                };

                Object {
                    key: Some(model.key.clone()),
                    size: Some(model.size.into()),
                    last_modified,
                    e_tag: model.etag.clone(),
                    ..Default::default()
                }
            })
            .collect();

        let key_count = contents.len() as u64;
        let is_truncated = key_count == limit;

        let next_marker = if is_truncated {
            contents.last().and_then(|obj| obj.key.clone())
        } else {
            None
        };

        let res = S3Response::new(ListObjectsOutput {
            contents: Some(contents),
            common_prefixes: None,
            is_truncated: Some(is_truncated),
            marker: req.input.marker,
            next_marker,
            max_keys: Some(limit as i32),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let mut query = entity::metadata::Entity::find()
            .filter(entity::metadata::Column::Bucket.eq(req.input.bucket.clone()))
            .order_by_asc(entity::metadata::Column::Key);

        if let Some(prefix) = req.input.prefix {
            query = query.filter(entity::metadata::Column::Key.starts_with(prefix.clone()));
        }

        if let Some(token) = req.input.continuation_token {
            query = query.filter(entity::metadata::Column::Key.gt(token));
        }

        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let items = query
            .limit(Some(limit))
            .select_only()
            .columns([
                entity::metadata::Column::Key,
                entity::metadata::Column::Size,
                entity::metadata::Column::LastModified,
                entity::metadata::Column::Etag,
            ])
            .all(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let contents: Vec<Object> = items
            .iter()
            .map(|model| {
                let last_modified = {
                    let last_modified: SystemTime = model.last_modified.into();
                    let last_modified = Timestamp::from(last_modified);

                    Some(last_modified)
                };

                Object {
                    key: Some(model.key.clone()),
                    size: Some(model.size.into()),
                    last_modified: last_modified,
                    e_tag: model.etag.clone(),
                    ..Default::default()
                }
            })
            .collect();

        let next_token = if contents.len() as u64 == limit {
            contents.last().and_then(|obj| obj.key.clone())
        } else {
            None
        };

        let is_truncated = next_token.is_some();

        let res = S3Response::new(ListObjectsV2Output {
            contents: Some(contents),
            common_prefixes: None,
            is_truncated: Some(is_truncated),
            next_continuation_token: next_token,
            key_count: Some(items.len() as i32),
            max_keys: Some(limit as i32),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            ..Default::default()
        });

        Ok(res)
    }
}
