use std::{ops::Deref, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use s3s::{
    Body, S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
        DeleteObjectInput, DeleteObjectOutput, DeleteObjectsInput, DeleteObjectsOutput,
        DeletedObject, GetObjectInput, GetObjectOutput, HeadObjectInput, HeadObjectOutput,
        ListBucketsInput, ListBucketsOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput,
        Timestamp,
    },
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, sea_query::OnConflict,
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
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let active_model = entity::bucket::ActiveModel {
            id: Set(req.input.bucket.clone()),
            region: Set(req.region),
            created_at: Set(chrono::Local::now().to_utc()),
        };

        match entity::bucket::Entity::insert(active_model)
            .exec(&self.inner)
            .await
        {
            Ok(_) => {
                let res = S3Response::new(CreateBucketOutput::default());
                Ok(res)
            }
            Err(DbErr::Query(err)) if err.to_string().contains("Duplicate entry") => {
                Err(S3Error::new(S3ErrorCode::BucketAlreadyExists))
            }
            Err(_) => Err(S3Error::new(S3ErrorCode::BucketAlreadyExists)),
        }
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = entity::bucket::Entity::find()
            .all(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let buckets: Vec<Bucket> = buckets
            .into_iter()
            .map(|model| {
                let creation_date = {
                    let creation_date: SystemTime = model.created_at.into();
                    let creation_date = Timestamp::from(creation_date);

                    Some(creation_date)
                };

                Bucket {
                    name: Some(model.id),
                    creation_date,
                    bucket_region: model.region,
                }
            })
            .collect();

        let res = S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        });

        Ok(res)
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket_name = req.input.bucket;

        let bucket_exists = entity::bucket::Entity::find_by_id(bucket_name.clone())
            .one(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .is_some();

        if !bucket_exists {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }

        let object_count = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(bucket_name.clone()))
            .count(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        if object_count > 0 {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }

        entity::bucket::Entity::delete_by_id(bucket_name)
            .exec(&self.inner)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let res = S3Response::new(DeleteBucketOutput::default());
        Ok(res)
    }

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

        let active_model = entity::object::ActiveModel {
            bucket_id: Set(req.input.bucket),
            id: Set(req.input.key),
            size: Set(body.len() as u32),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(req.input.content_type.map(|v| v.to_string())),
            etag: Set(Some(etag.clone())),
            content: Set(content),
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
        let model = entity::object::Entity::find_by_id((req.input.bucket, req.input.key))
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
        let model = entity::object::Entity::find_by_id((req.input.bucket, req.input.key))
            .select_only()
            .columns([
                entity::object::Column::Size,
                entity::object::Column::ContentType,
                entity::object::Column::Etag,
                entity::object::Column::LastModified,
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
        entity::object::Entity::delete_by_id((req.input.bucket, req.input.key))
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

        entity::object::Entity::delete_many()
            .filter(entity::object::Column::BucketId.eq(req.input.bucket.clone()))
            .filter(entity::object::Column::Id.is_in(keys.clone()))
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
        let mut query = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(req.input.bucket.clone()))
            .order_by_asc(entity::object::Column::Id);

        if let Some(prefix) = &req.input.prefix {
            query = query.filter(entity::object::Column::Id.starts_with(prefix.clone()));
        }

        if let Some(marker) = &req.input.marker {
            query = query.filter(entity::object::Column::Id.gt(marker.clone()));
        }

        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let items = query
            .limit(Some(limit))
            .select_only()
            .columns([
                entity::object::Column::Id,
                entity::object::Column::Size,
                entity::object::Column::LastModified,
                entity::object::Column::Etag,
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
                    key: Some(model.id.clone()),
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
        let mut query = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(req.input.bucket.clone()))
            .order_by_asc(entity::object::Column::Id);

        if let Some(prefix) = req.input.prefix.clone() {
            query = query.filter(entity::object::Column::Id.starts_with(prefix));
        }

        if let Some(token) = req.input.continuation_token {
            query = query.filter(entity::object::Column::Id.gt(token));
        }

        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let items = query
            .limit(Some(limit))
            .select_only()
            .columns([
                entity::object::Column::Id,
                entity::object::Column::Size,
                entity::object::Column::LastModified,
                entity::object::Column::Etag,
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
                    key: Some(model.id.clone()),
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
