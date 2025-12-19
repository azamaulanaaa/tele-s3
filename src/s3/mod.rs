use std::{str::FromStr, time::SystemTime};

use futures::{StreamExt, TryStreamExt};
use grammers_client::client::files::MAX_CHUNK_SIZE;
use s3s::{
    S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{
        AbortMultipartUploadInput, AbortMultipartUploadOutput, CommonPrefix,
        CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CreateBucketInput,
        CreateBucketOutput, CreateMultipartUploadInput, CreateMultipartUploadOutput,
        DeleteBucketInput, DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        GetObjectInput, GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput,
        HeadObjectOutput, ListObjectsInput, ListObjectsOutput, ListObjectsV2Input,
        ListObjectsV2Output, Object, PutObjectInput, PutObjectOutput, StreamingBlob, Timestamp,
        UploadPartInput, UploadPartOutput,
    },
};
use sea_orm::DatabaseConnection;
use serde::{Deserialize, Serialize};

use crate::{
    grammers::{Grammers, MessageId},
    s3::metadata::{Metadata, MetadataDb, MetadataStorage, MetadataStorageError},
};

mod metadata;

#[derive(Debug, thiserror::Error)]
pub enum TeleS3Error {
    #[error("Metadata Storage : {0}")]
    MetadataStorage(#[from] MetadataStorageError),
}

pub struct TeleS3 {
    metadata_storage: MetadataDb,
    grammers: Grammers,
}

impl TeleS3 {
    pub async fn init(
        grammers: Grammers,
        connection: DatabaseConnection,
    ) -> Result<Self, TeleS3Error> {
        let metadata_storage = MetadataDb::init(connection).await?;

        Ok(Self {
            metadata_storage,
            grammers: grammers,
        })
    }
}

#[async_trait::async_trait]
impl S3 for TeleS3 {
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        Err(S3Error::new(S3ErrorCode::MethodNotAllowed))
    }

    async fn delete_bucket(
        &self,
        _req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        Err(S3Error::new(S3ErrorCode::MethodNotAllowed))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        let bucket = req.input.bucket;

        let exists = self
            .grammers
            .get_peer_by_username(&bucket)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        if exists.is_some() {
            Ok(S3Response::new(HeadBucketOutput::default()))
        } else {
            Err(S3Error::new(S3ErrorCode::NoSuchBucket))
        }
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let peer = self
            .grammers
            .get_peer_by_username(&req.input.bucket)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchBucket))?;

        let size = req
            .input
            .content_length
            .ok_or(S3Error::new(S3ErrorCode::MissingContentLength))? as usize;

        let body_stream = req
            .input
            .body
            .ok_or(S3Error::new(S3ErrorCode::IncompleteBody))?;

        let mut stream = {
            fn map_err(e: Box<dyn std::error::Error + Sync + Send>) -> std::io::Error {
                std::io::Error::other(e)
            }

            let stream = body_stream.map_err(map_err).into_async_read();
            let stream = Box::pin(stream);
            stream
        };

        let message_id = self
            .grammers
            .upload_document(&mut stream, size, req.input.key.clone(), peer)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
        let inner_item = MetadataInnerItem {
            message_id,
            size: size as i64,
        };
        let inner_json = serde_json::to_value(vec![inner_item])
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let metadata = Metadata {
            key: req.input.key,
            bucket: req.input.bucket,
            size: size as u64,
            last_modified: chrono::Utc::now(),
            content_type: req.input.content_type.map(|v| v.to_string()),
            etag: None,
            inner: inner_json,
        };

        self.metadata_storage
            .put(metadata)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        Ok(S3Response::new(PutObjectOutput::default()))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let _ = self
            .grammers
            .get_peer_by_username(&req.input.bucket)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchBucket))?;

        let upload_id = uuid::Uuid::new_v4().to_string();

        let output = CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(upload_id),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        let peer = self
            .grammers
            .get_peer_by_username(&req.input.bucket)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchBucket))?;

        let size = req
            .input
            .content_length
            .ok_or(S3Error::new(S3ErrorCode::MissingContentLength))? as usize;
        let body_stream = req
            .input
            .body
            .ok_or(S3Error::new(S3ErrorCode::IncompleteBody))?;

        let upload_id = req.input.upload_id;
        let part_number = req.input.part_number;

        let mut stream = {
            fn map_err(e: Box<dyn std::error::Error + Sync + Send>) -> std::io::Error {
                std::io::Error::other(e)
            }
            Box::pin(body_stream.map_err(map_err).into_async_read())
        };

        let name = format!("{}.{:03}", req.input.key.clone(), part_number);
        let message_id = self
            .grammers
            .upload_document(&mut stream, size, name, peer)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
        let inner_item = MetadataInnerItem {
            message_id,
            size: size as i64,
        };
        let inner_json = serde_json::to_value(vec![inner_item])
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let part_key = format!("{}_part_{}_{}", req.input.key, upload_id, part_number);

        let metadata = Metadata {
            key: part_key,
            bucket: req.input.bucket,
            size: size as u64,
            last_modified: chrono::Utc::now(),
            content_type: None,
            etag: Some(format!("\"{upload_id}-{part_number}\"")),
            inner: inner_json,
        };

        self.metadata_storage
            .put(metadata)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let output = UploadPartOutput {
            e_tag: Some(format!("\"{upload_id}-{part_number}\"")),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let upload_id = req.input.upload_id;
        let multipart = req
            .input
            .multipart_upload
            .ok_or(S3Error::new(S3ErrorCode::InvalidRequest))?;
        let mut parts = multipart
            .parts
            .ok_or(S3Error::new(S3ErrorCode::InvalidRequest))?;

        parts.sort_by_key(|p| p.part_number);

        let mut final_inner = Vec::new();
        let mut total_size: u64 = 0;

        for part in parts {
            let part_number = part
                .part_number
                .ok_or(S3Error::new(S3ErrorCode::InvalidRequest))?;
            let part_key = format!("{}_part_{}_{}", req.input.key, upload_id, part_number);

            let metadata = self
                .metadata_storage
                .get(&req.input.bucket, &part_key)
                .await
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
                .ok_or(S3Error::new(S3ErrorCode::InvalidPart))?;

            let inner: Vec<MetadataInnerItem> = serde_json::from_value(metadata.inner)
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

            final_inner.push(inner);
            total_size += metadata.size;

            self.metadata_storage
                .delete(&req.input.bucket, &part_key)
                .await
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
        }
        let final_inners = final_inner.concat();

        let inner_json = serde_json::to_value(final_inners)
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let final_metadata = Metadata {
            key: req.input.key.clone(),
            bucket: req.input.bucket.clone(),
            size: total_size,
            last_modified: chrono::Utc::now(),
            content_type: None,
            etag: Some(format!("\"{}-{}\"", upload_id, total_size)),
            inner: inner_json,
        };

        self.metadata_storage
            .put(final_metadata)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            e_tag: None,
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let prefix = format!("{}_part_{}_", req.input.key, req.input.upload_id);

        let result = self
            .metadata_storage
            .list(&req.input.bucket, Some(&prefix), None, 1000, None)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        for metadata in result.metadatas {
            self.metadata_storage
                .delete(&req.input.bucket, &metadata.key)
                .await
                .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;
        }

        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        if req.input.range.is_some() {
            return Err(S3Error::new(S3ErrorCode::NotImplemented));
        }

        let metadata = self
            .metadata_storage
            .get(&req.input.bucket, &req.input.key)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchKey))?;

        let inner: Vec<MetadataInnerItem> = serde_json::from_value(metadata.inner)
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let peer = self
            .grammers
            .get_peer_by_username(&req.input.bucket)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchBucket))?;

        let download_futures = inner.into_iter().map(|item| {
            self.grammers
                .download_document(peer.clone(), item.message_id, MAX_CHUNK_SIZE)
        });

        let streams: Vec<_> = futures::future::try_join_all(download_futures)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let merged_stream = futures::stream::iter(streams)
            .flatten()
            .map(|chunk_res| chunk_res.map(|vec| bytes::Bytes::from(vec)));

        let body = StreamingBlob::wrap(merged_stream);

        let content_type = metadata
            .content_type
            .map(|v| mime::Mime::from_str(&v))
            .transpose()
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let last_modified = {
            let last_modified: SystemTime = metadata.last_modified.into();
            let last_modified = Timestamp::from(last_modified);

            Some(last_modified)
        };

        let output = GetObjectOutput {
            body: Some(body),
            content_length: Some(metadata.size as i64),
            content_type,
            last_modified,
            e_tag: metadata.etag,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let metadata = self
            .metadata_storage
            .get(&req.input.bucket, &req.input.key)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?
            .ok_or(S3Error::new(S3ErrorCode::NoSuchKey))?;

        let content_type = metadata
            .content_type
            .map(|v| mime::Mime::from_str(&v))
            .transpose()
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let last_modified = {
            let last_modified: SystemTime = metadata.last_modified.into();
            let last_modified = Timestamp::from(last_modified);
            Some(last_modified)
        };

        let output = HeadObjectOutput {
            accept_ranges: Some("none".to_string()),
            content_length: Some(metadata.size as i64),
            content_type,
            last_modified,
            e_tag: metadata.etag,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.metadata_storage
            .delete(&req.input.bucket, &req.input.key)
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let limit = req.input.max_keys.map(|m| m as u64).unwrap_or(1000);

        let result = self
            .metadata_storage
            .list(
                &req.input.bucket,
                req.input.prefix.as_deref(),
                req.input.delimiter.as_deref(),
                limit,
                req.input.marker.as_deref(),
            )
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let contents: Vec<Object> = result
            .metadatas
            .into_iter()
            .map(|m| {
                let last_modified = {
                    let last_modified: SystemTime = m.last_modified.into();
                    let last_modified = Timestamp::from(last_modified);
                    Some(last_modified)
                };

                Object {
                    key: Some(m.key),
                    size: Some(m.size as i64),
                    last_modified,
                    e_tag: m.etag,
                    owner: None,
                    storage_class: None,
                    ..Default::default()
                }
            })
            .collect();

        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefix
            .into_iter()
            .map(|p| CommonPrefix { prefix: Some(p) })
            .collect();

        let output = ListObjectsOutput {
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            delimiter: req.input.delimiter,
            max_keys: Some(limit as i32),
            marker: req.input.marker,

            contents: Some(contents),
            common_prefixes: Some(common_prefixes),

            is_truncated: Some(result.next_token.is_some()),
            next_marker: result.next_token,

            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let limit = req.input.max_keys.map(|m| m as u64).unwrap_or(1000);
        let result = self
            .metadata_storage
            .list(
                &req.input.bucket,
                req.input.prefix.as_deref(),
                req.input.delimiter.as_deref(),
                limit,
                req.input.continuation_token.as_deref(),
            )
            .await
            .map_err(|_| S3Error::new(S3ErrorCode::InternalError))?;

        let contents: Vec<Object> = result
            .metadatas
            .into_iter()
            .map(|m| {
                let last_modified = {
                    let last_modified: SystemTime = m.last_modified.into();
                    let last_modified = Timestamp::from(last_modified);

                    Some(last_modified)
                };

                Object {
                    key: Some(m.key),
                    size: Some(m.size as i64),
                    last_modified,
                    e_tag: m.etag,
                    owner: None,
                    storage_class: None,
                    checksum_algorithm: None,
                    checksum_type: None,
                    restore_status: None,
                }
            })
            .collect();

        let common_prefixes: Vec<CommonPrefix> = result
            .common_prefix
            .into_iter()
            .map(|p| CommonPrefix { prefix: Some(p) })
            .collect();

        let output = ListObjectsV2Output {
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            is_truncated: Some(result.next_token.is_some()),
            next_continuation_token: result.next_token,
            key_count: None,
            max_keys: Some(limit as i32),
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            delimiter: req.input.delimiter,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetadataInnerItem {
    message_id: MessageId,
    size: i64,
}
