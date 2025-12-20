use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    pin::Pin,
    str::FromStr,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::SystemTime,
};

use bytes::Bytes;
use futures::{Stream, TryStreamExt, io::AsyncRead};
use mime::Mime;
use s3s::{
    S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, GetObjectInput, GetObjectOutput,
        HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListBucketsInput,
        ListBucketsOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp,
        UploadPartInput, UploadPartOutput,
    },
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, SqlErr, sea_query::OnConflict,
};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::backend::{Backend, BoxedAsyncReader};

mod entity;

pub struct TeleS3<B: Backend> {
    backend: B,
    pending_uploads: Arc<tokio::sync::Mutex<HashMap<MultipartUploadKey, MultipartUploadState>>>,
    db: DatabaseConnection,
}

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(backend, db), err)]
    pub async fn init(backend: B, db: DatabaseConnection) -> anyhow::Result<Self> {
        Ok(Self {
            backend,
            pending_uploads: Default::default(),
            db,
        })
    }
}

#[async_trait::async_trait]
impl<B: Backend> S3 for TeleS3<B> {
    #[instrument(skip(self), err)]
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
            .exec(&self.db)
            .await
        {
            Ok(_) => {
                let res = S3Response::new(CreateBucketOutput::default());
                Ok(res)
            }
            Err(DbErr::Exec(e)) => {
                let err = DbErr::Exec(e);

                match err.sql_err() {
                    Some(SqlErr::UniqueConstraintViolation(_)) => {
                        Err(S3Error::new(S3ErrorCode::BucketAlreadyExists))
                    }
                    _ => Err(S3Error::internal_error(err)),
                }
            }

            Err(e) => Err(S3Error::internal_error(e)),
        }
    }

    #[instrument(skip(self), err)]
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = entity::bucket::Entity::find()
            .all(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let buckets: Vec<Bucket> = buckets
            .into_iter()
            .map(|model| Bucket {
                name: Some(model.id),
                creation_date: Some(chrono_to_timestamp(model.created_at)),
                bucket_region: model.region,
            })
            .collect();

        let res = S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        let bucket_exists = entity::bucket::Entity::find_by_id(req.input.bucket.clone())
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .is_some();

        if !bucket_exists {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }

        let object_count = entity::object::Entity::find()
            .filter(entity::object::Column::BucketId.eq(req.input.bucket.clone()))
            .count(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        if object_count > 0 {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }

        entity::bucket::Entity::delete_by_id(req.input.bucket)
            .exec(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let res = S3Response::new(DeleteBucketOutput::default());

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let exists = entity::bucket::Entity::find_by_id(req.input.bucket)
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .is_some();

        if !exists {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }

        let res = S3Response::new(HeadBucketOutput::default());
        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn put_object(
        &self,
        mut req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let size =
            req.input
                .content_length
                .ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))? as u64;

        let body_stream = req
            .input
            .body
            .take()
            .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

        let id = self
            .backend
            .write(size as u64, body_stream.into_boxed_reader())
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let metadata = Metadata {
            item: vec![MetadataItem { id, size }],
        };

        let metadata_json =
            serde_json::to_value(&metadata).map_err(|e| S3Error::internal_error(e))?;

        let active_model = entity::object::ActiveModel {
            bucket_id: Set(req.input.bucket),
            id: Set(req.input.key),
            size: Set(size as u32),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(req.input.content_type.map(|v| v.to_string())),
            etag: Set(None),
            content: Set(metadata_json),
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
            .exec(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let res = S3Response::new(PutObjectOutput {
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let upload_id = uuid::Uuid::new_v4().to_string();

        let key = MultipartUploadKey {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id: upload_id.clone(),
        };

        let state = MultipartUploadState {
            content_type: req.input.content_type,
            parts: BTreeMap::new(),
        };

        {
            let mut uploads = self.pending_uploads.lock().await;

            uploads.insert(key, state);
        }

        let res = S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn upload_part(
        &self,
        mut req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let key = MultipartUploadKey {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id: req.input.upload_id,
        };

        {
            let uploads = self.pending_uploads.lock().await;

            if !uploads.contains_key(&key) {
                return Err(S3Error::new(S3ErrorCode::NoSuchUpload));
            }
        }

        let size =
            req.input
                .content_length
                .ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))? as u64;

        let body_stream = req
            .input
            .body
            .take()
            .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

        let id = self
            .backend
            .write(size, body_stream.into_boxed_reader())
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        {
            let mut uploads = self.pending_uploads.lock().await;

            if let Some(state) = uploads.get_mut(&key) {
                state
                    .parts
                    .insert(req.input.part_number, MetadataItem { id, size });
            } else {
                let _ = self.backend.delete(id).await;
                return Err(S3Error::new(S3ErrorCode::NoSuchUpload));
            }
        }

        let output = UploadPartOutput {
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    #[instrument(skip(self), err)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let key = MultipartUploadKey {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id: req.input.upload_id,
        };

        let requested_parts = req
            .input
            .multipart_upload
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?
            .parts
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

        let state = {
            let mut uploads = self.pending_uploads.lock().await;

            uploads
                .remove(&key)
                .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?
        };

        let mut metadata_items = Vec::new();

        for req_part in requested_parts {
            let part_num = req_part
                .part_number
                .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidRequest))?;

            let metadata_item = state
                .parts
                .get(&part_num)
                .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

            metadata_items.push(metadata_item.clone());
        }

        let size = metadata_items.iter().map(|v| v.size).sum::<u64>() as u32;

        let metadata = Metadata {
            item: metadata_items,
        };
        let metadata_json =
            serde_json::to_value(&metadata).map_err(|e| S3Error::internal_error(e))?;

        let active_model = entity::object::ActiveModel {
            bucket_id: Set(req.input.bucket),
            id: Set(req.input.key),
            size: Set(size),
            last_modified: Set(chrono::Local::now().to_utc()),
            content_type: Set(state.content_type.map(|v| v.to_string())),
            etag: Set(None),
            content: Set(metadata_json),
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
            .exec(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let res = S3Response::new(CompleteMultipartUploadOutput {
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let key = MultipartUploadKey {
            bucket: req.input.bucket.clone(),
            key: req.input.key.clone(),
            upload_id: req.input.upload_id,
        };

        let state = {
            let mut uploads = self.pending_uploads.lock().await;

            uploads
                .remove(&key)
                .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?
        };

        let delete_futures = state
            .parts
            .values()
            .map(|part| self.backend.delete(part.id.clone()));

        let _ = futures::future::join_all(delete_futures).await;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[instrument(skip(self), err)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = entity::object::Entity::find_by_id((req.input.bucket, req.input.key))
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(|e| S3Error::internal_error(e))?;

        let (mut offset, mut remain_length) = if let Some(range) = req.input.range {
            let r = range.check(model.size as u64)?;
            (r.start, r.end - r.start)
        } else {
            (0, model.size as u64)
        };

        let content_length = remain_length;

        let reader_futures = metadata.item.into_iter().filter_map(|item| {
            if remain_length == 0 {
                return None;
            }

            let item_size = item.size as u64;

            if offset >= item_size {
                offset -= item_size;
                return None;
            }

            let local_offset = offset;
            let bytes_available = item_size - local_offset;
            let take_amount = std::cmp::min(bytes_available, remain_length);

            offset = 0;
            remain_length -= take_amount;

            let reader = self.backend.read(item.id, local_offset, Some(take_amount));

            Some(reader)
        });

        let readers = futures::future::try_join_all(reader_futures)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InternalError))?;

        let chain_readers = ChainReaders::from_vec(readers);

        let body = StreamingBlob::wrap(chain_readers);

        let res = S3Response::new(GetObjectOutput {
            content_type: model.content_type.map(|v| v.parse().ok()).flatten(),
            content_length: Some(content_length as i64),
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag,
            body: Some(body),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let model = entity::object::Entity::find_by_id((req.input.bucket, req.input.key))
            .one(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let res = S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(model.size as i64),
            content_type: model
                .content_type
                .map(|v| Mime::from_str(&v).ok())
                .flatten(),
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag,
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let model =
            entity::object::Entity::find_by_id((req.input.bucket.clone(), req.input.key.clone()))
                .one(&self.db)
                .await
                .map_err(|e| S3Error::internal_error(e))?
                .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(|e| S3Error::internal_error(e))?;

        entity::object::Entity::delete_by_id((req.input.bucket, req.input.key))
            .exec(&self.db)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        futures::future::try_join_all(
            metadata
                .item
                .iter()
                .map(|item| self.backend.delete(item.id.clone())),
        )
        .await
        .map_err(|e| S3Error::internal_error(e))?;

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

        let models = entity::object::Entity::delete_many()
            .filter(entity::object::Column::BucketId.eq(req.input.bucket.clone()))
            .filter(entity::object::Column::Id.is_in(keys.clone()))
            .exec_with_returning(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let quiet = req.input.delete.quiet.unwrap_or(false);

        let deleted = if quiet {
            None
        } else {
            let deleted_objects: Vec<DeletedObject> = models
                .iter()
                .map(|model| DeletedObject {
                    key: Some(model.id.clone()),
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

    #[instrument(skip(self), err)]
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
            .all(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let contents: Vec<Object> = items
            .iter()
            .map(|model| Object {
                key: Some(model.id.clone()),
                size: Some(model.size.into()),
                last_modified: Some(chrono_to_timestamp(model.last_modified)),
                ..Default::default()
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

    #[instrument(skip(self), err)]
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
            .all(&self.db)
            .await
            .map_err(|e| S3Error::internal_error(e))?;

        let contents: Vec<Object> = items
            .iter()
            .map(|model| Object {
                key: Some(model.id.clone()),
                size: Some(model.size.into()),
                last_modified: Some(chrono_to_timestamp(model.last_modified)),
                e_tag: model.etag.clone(),
                ..Default::default()
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metadata {
    item: Vec<MetadataItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataItem {
    id: String,
    size: u64,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct MultipartUploadKey {
    bucket: String,
    key: String,
    upload_id: String,
}

#[derive(Debug, Clone)]
struct MultipartUploadState {
    content_type: Option<Mime>,
    parts: BTreeMap<i32, MetadataItem>,
}

pub struct ChainReaders {
    readers: Mutex<VecDeque<BoxedAsyncReader>>,
    buffer: Box<[u8]>,
}

impl ChainReaders {
    pub fn from_vec(readers: Vec<BoxedAsyncReader>) -> Self {
        Self {
            readers: Mutex::new(VecDeque::from(readers)),
            buffer: vec![0u8; 4096].into_boxed_slice(),
        }
    }
}

impl Stream for ChainReaders {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let buffer = &mut this.buffer;
        let readers_mutex = &mut this.readers;

        let mut readers = match readers_mutex.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return Poll::Pending;
            }
        };

        loop {
            let reader = match readers.front_mut() {
                Some(r) => r,
                None => return Poll::Ready(None),
            };

            match Pin::new(reader).poll_read(cx, buffer) {
                Poll::Ready(Ok(0)) => {
                    readers.pop_front();
                    continue;
                }
                Poll::Ready(Ok(n)) => {
                    let data = Bytes::copy_from_slice(&buffer[..n]);
                    return Poll::Ready(Some(Ok(data)));
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();
    let datetime = Timestamp::from(datetime);

    datetime
}

trait StreamingBlobExt {
    fn into_boxed_reader(self) -> BoxedAsyncReader;
}

impl StreamingBlobExt for StreamingBlob {
    fn into_boxed_reader(self) -> BoxedAsyncReader {
        let stream = self.map_err(|e| std::io::Error::other(e)).into_async_read();
        Box::pin(stream)
    }
}
