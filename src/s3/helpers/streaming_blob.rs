//! Adapter that converts a streamed request body to the crate's reader
//! type.
//!
//! This adapter bridges `s3s`' body type and [`BoxedAsyncReader`]. The S3
//! handlers never name a concrete reader. The code computes digests by
//! wrapping the reader that comes from this adapter. It does not change
//! the adapter.

use futures::TryStreamExt;
use s3s::dto::StreamingBlob;

use crate::backend::BoxedAsyncReader;

pub(crate) trait StreamingBlobExt {
    fn into_boxed_reader(self) -> BoxedAsyncReader;
}

impl StreamingBlobExt for StreamingBlob {
    fn into_boxed_reader(self) -> BoxedAsyncReader {
        let stream = self.map_err(std::io::Error::other).into_async_read();
        Box::pin(stream)
    }
}
