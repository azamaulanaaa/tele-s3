//! Adapter turning a streamed request body into the crate's reader type.
//!
//! Bridges `s3s`' body type and [`BoxedAsyncReader`] so the S3 handlers never
//! name a concrete reader. Digests are computed by wrapping what comes out of
//! here, not by changing this adapter.

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
