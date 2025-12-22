use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::SystemTime,
};

use bytes::Bytes;
use digest::Digest;
use futures::{Stream, TryStreamExt, io::AsyncRead};
use s3s::{
    S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, Bucket,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectsInput, DeleteObjectsOutput, DeletedObject, ETag, GetObjectInput,
        GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
        ListBucketsInput, ListBucketsOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput,
        StreamingBlob, Timestamp, UploadPartInput, UploadPartOutput,
    },
};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{
    backend::{Backend, BoxedAsyncReader, ReaderWithHasher},
    s3::repo::entity,
};
use repo::Repository;

mod repo;

pub struct TeleS3<B: Backend> {
    backend: B,
    repo: Repository,
}

impl<B: Backend> TeleS3<B> {
    #[instrument(skip(backend, db), level = "debug", err)]
    pub async fn init(backend: B, db: DatabaseConnection) -> anyhow::Result<Self> {
        let repo = Repository::init(db).await?;

        Ok(Self { backend, repo })
    }
}

#[async_trait::async_trait]
impl<B: Backend> S3 for TeleS3<B> {
    #[instrument(skip(self), err)]
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.repo
            .create_bucket(req.input.bucket, req.region)
            .await?;

        let res = S3Response::new(CreateBucketOutput::default());

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.repo.list_buckets().await?;

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
        let is_exists = self.repo.bucket_exists(&req.input.bucket).await?;
        if !is_exists {
            return Err(S3Error::new(S3ErrorCode::NoSuchBucket));
        }

        let object_count = self.repo.get_bucket_object_count(&req.input.bucket).await?;
        if object_count > 0 {
            return Err(S3Error::new(S3ErrorCode::BucketNotEmpty));
        }

        self.repo.delete_bucket(&req.input.bucket).await?;

        let res = S3Response::new(DeleteBucketOutput::default());

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let is_exists = self.repo.bucket_exists(&req.input.bucket).await?;
        if !is_exists {
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

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        let (id, hash_md5) = {
            let hasher_md5 = Arc::new(Mutex::new(md5::Md5::new()));

            let reader_with_hasher = Box::pin(ReaderWithHasher::new(reader, hasher_md5.clone()));

            let id = self
                .backend
                .write(size, reader_with_hasher)
                .await
                .map_err(S3Error::internal_error)?;

            let hash_md5 = hasher_md5
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .finalize_reset();
            let hash_md5 = hex::encode(hash_md5);

            (id, hash_md5)
        };

        if req.input.content_md5 != Some(hash_md5.clone()) {
            let _ = self.backend.delete(id).await;

            return Err(S3Error::new(S3ErrorCode::BadDigest));
        }

        let etag = Some(format!("\"{}\"", hash_md5));

        let content_json = {
            let content = Metadata {
                item: vec![MetadataItem {
                    id: id.clone(),
                    size,
                }],
            };

            serde_json::to_value(&content).map_err(S3Error::internal_error)?
        };

        let delete_old_future = {
            let is_exists = self
                .repo
                .object_exists(&req.input.bucket, &req.input.key)
                .await?;

            if is_exists {
                let model = self
                    .repo
                    .get_object(&req.input.bucket, &req.input.key)
                    .await?;

                let metadata: Metadata =
                    serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
                let delete_futures = metadata.item.into_iter().map(|v| self.backend.delete(v.id));

                Some(delete_futures)
            } else {
                None
            }
        };

        {
            let result = self
                .repo
                .upsert_object(
                    req.input.bucket,
                    req.input.key,
                    size,
                    req.input.content_type,
                    etag.clone(),
                    content_json,
                )
                .await;

            if let Err(err) = result {
                let _ = self.backend.delete(id).await;

                return Err(err);
            }
        }

        if let Some(delete_future) = delete_old_future {
            let _ = futures::future::join_all(delete_future).await;
        }

        let res = S3Response::new(PutObjectOutput {
            e_tag: etag.map(ETag::Strong),
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

        let content = BTreeMap::<i32, MultipartUploadPart>::new();
        let content_json = serde_json::to_value(&content).map_err(S3Error::internal_error)?;

        self.repo
            .upsert_multipart_upload_state(
                req.input.bucket.clone(),
                req.input.key.clone(),
                upload_id.clone(),
                req.input.content_type,
                content_json,
            )
            .await?;

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
        let _model = self
            .repo
            .get_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?;

        let size =
            req.input
                .content_length
                .ok_or_else(|| S3Error::new(S3ErrorCode::MissingContentLength))? as u64;

        let reader = {
            let body_stream = req
                .input
                .body
                .take()
                .ok_or_else(|| S3Error::new(S3ErrorCode::IncompleteBody))?;

            body_stream.into_boxed_reader()
        };

        let (id, hash_md5) = {
            let hasher_md5 = Arc::new(Mutex::new(md5::Md5::new()));

            let reader_with_hasher = Box::pin(ReaderWithHasher::new(reader, hasher_md5.clone()));

            let id = self
                .backend
                .write(size, reader_with_hasher)
                .await
                .map_err(S3Error::internal_error)?;

            let hash_md5 = hasher_md5
                .lock()
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .finalize_reset();
            let hash_md5 = hex::encode(hash_md5);

            (id, hash_md5)
        };

        if req.input.content_md5 != Some(hash_md5.clone()) {
            let _ = self.backend.delete(id).await;

            return Err(S3Error::new(S3ErrorCode::BadDigest));
        }

        let multipart_upload_part = MultipartUploadPart {
            hash: hash_md5,
            metadata_item: MetadataItem { id, size },
        };

        self.repo
            .update_multipart_upload_state(
                &req.input.bucket,
                &req.input.key,
                &req.input.upload_id,
                |model| {
                    let mut content: BTreeMap<i32, MultipartUploadPart> =
                        serde_json::from_value(model.content.clone())
                            .map_err(S3Error::internal_error)?;

                    content.insert(req.input.part_number, multipart_upload_part.clone());

                    let content_json =
                        serde_json::to_value(&content).map_err(S3Error::internal_error)?;

                    let mut active_model: entity::multipart_upload_state::ActiveModel =
                        model.into();
                    active_model.content = sea_orm::Set(content_json);

                    Ok(active_model)
                },
            )
            .await?;

        let res = S3Response::new(UploadPartOutput {
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let requested_parts = req
            .input
            .multipart_upload
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?
            .parts
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?
            .into_iter()
            .map(|v| v.part_number)
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

        let model = self
            .repo
            .delete_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        let mut content =
            serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(model.content)
                .map_err(S3Error::internal_error)?;

        let filtered_content = requested_parts
            .iter()
            .map(|index| content.remove(index))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidPart))?;

        let metadata_json = {
            let metadata_items = filtered_content
                .iter()
                .map(|v| v.metadata_item.clone())
                .collect::<Vec<_>>();
            let metadata = Metadata {
                item: metadata_items,
            };

            serde_json::to_value(&metadata).map_err(S3Error::internal_error)?
        };

        let size = filtered_content.iter().map(|v| v.metadata_item.size).sum();
        let etag = {
            let part_count = filtered_content.len();

            let hashes_byte = filtered_content
                .iter()
                .map(|v| hex::decode(&v.hash))
                .collect::<Result<Vec<_>, _>>()
                .map_err(S3Error::internal_error)?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            let hash_md5 = md5::Md5::digest(&hashes_byte);
            let hash_md5 = hex::encode(hash_md5);

            format!("\"{}-{}\"", hash_md5, part_count)
        };

        let delete_old_object_futures = {
            let model = self
                .repo
                .get_object(&req.input.bucket, &req.input.key)
                .await;
            if let Ok(model) = model {
                let metadata: Metadata =
                    serde_json::from_value(model.content).map_err(S3Error::internal_error)?;
                let delete_futures = metadata.item.into_iter().map(|v| self.backend.delete(v.id));

                Some(delete_futures)
            } else {
                None
            }
        };

        self.repo
            .upsert_object(
                req.input.bucket.clone(),
                req.input.key.clone(),
                size,
                model.content_type,
                Some(etag),
                metadata_json,
            )
            .await?;

        if let Some(delete_futures) = delete_old_object_futures {
            let _ = futures::future::join_all(delete_futures).await;
        }

        let delete_dangling_futures = content.into_values().map(|multipart_upload_part| {
            self.backend.delete(multipart_upload_part.metadata_item.id)
        });
        let _ = futures::future::join_all(delete_dangling_futures).await;

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
        let model = self
            .repo
            .delete_multipart_upload_state(&req.input.bucket, &req.input.key, &req.input.upload_id)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchUpload))?;

        let content = serde_json::from_value::<BTreeMap<i32, MultipartUploadPart>>(model.content)
            .map_err(S3Error::internal_error)?;

        let delete_futures = content.into_values().map(|multipart_upload_part| {
            self.backend.delete(multipart_upload_part.metadata_item.id)
        });
        let _ = futures::future::join_all(delete_futures).await;

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    #[instrument(skip(self), err)]
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let model = self
            .repo
            .get_object(&req.input.bucket, &req.input.key)
            .await?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

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

            let item_size = item.size;

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
            content_type: model.content_type,
            content_length: Some(content_length as i64),
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
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
        let model = self
            .repo
            .get_object(&req.input.bucket, &req.input.key)
            .await?;

        let res = S3Response::new(HeadObjectOutput {
            accept_ranges: Some("bytes".to_string()),
            content_length: Some(model.size as i64),
            content_type: model.content_type,
            last_modified: Some(chrono_to_timestamp(model.last_modified)),
            e_tag: model.etag.map(ETag::Strong),
            ..Default::default()
        });

        Ok(res)
    }

    #[instrument(skip(self), err)]
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        let model = self
            .repo
            .delete_object(&req.input.bucket, &req.input.key)
            .await?
            .ok_or_else(|| S3Error::new(S3ErrorCode::NoSuchKey))?;

        let metadata: Metadata =
            serde_json::from_value(model.content).map_err(S3Error::internal_error)?;

        let delete_futures = metadata
            .item
            .iter()
            .map(|item| self.backend.delete(item.id.clone()));
        let _ = futures::future::join_all(delete_futures).await;

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

        let models = self.repo.delete_objects(&req.input.bucket, keys).await?;

        let metadatas: Vec<Metadata> = models
            .iter()
            .map(|model| {
                serde_json::from_value::<Metadata>(model.content.clone())
                    .map_err(S3Error::internal_error)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let delete_futures = metadatas
            .iter()
            .flat_map(|metadata| metadata.item.clone())
            .map(|item| self.backend.delete(item.id.clone()));
        let _ = futures::future::join_all(delete_futures).await;

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
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let models = self
            .repo
            .list_objects(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.marker.clone(),
                limit,
            )
            .await?;

        let contents: Vec<Object> = models
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
        let limit = req.input.max_keys.unwrap_or(1000) as u64;

        let items = self
            .repo
            .list_objects(
                &req.input.bucket,
                req.input.prefix.clone(),
                req.input.continuation_token,
                limit,
            )
            .await?;

        let contents: Vec<Object> = items
            .iter()
            .map(|model| Object {
                key: Some(model.id.clone()),
                size: Some(model.size.into()),
                last_modified: Some(chrono_to_timestamp(model.last_modified)),
                e_tag: model.etag.clone().map(ETag::Strong),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MultipartUploadPart {
    hash: String,
    metadata_item: MetadataItem,
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

        let readers = readers_mutex
            .get_mut()
            .map_err(|_| std::io::Error::other("reader poisoned"))?;

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

    Timestamp::from(datetime)
}

trait StreamingBlobExt {
    fn into_boxed_reader(self) -> BoxedAsyncReader;
}

impl StreamingBlobExt for StreamingBlob {
    fn into_boxed_reader(self) -> BoxedAsyncReader {
        let stream = self.map_err(std::io::Error::other).into_async_read();
        Box::pin(stream)
    }
}
