//! Adapter turning a streamed request body into the crate's reader type.

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
