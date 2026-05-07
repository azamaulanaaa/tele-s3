use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures::{Stream, io::AsyncRead};

use super::BoxedAsyncReader;

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
