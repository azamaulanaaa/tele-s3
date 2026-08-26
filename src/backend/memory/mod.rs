use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, RwLock};

use super::{Backend, BackendError, BoxedAsyncReader};

struct ObjectReader<const M: usize> {
    object: Arc<RwLock<Object<M>>>,
    end: Option<usize>,
    pos: usize,
}

impl<const M: usize> ObjectReader<M> {
    /// `limit` is a byte count relative to `start`; it is converted to an
    /// absolute end position so remaining-length math can never underflow.
    fn new(object: Arc<RwLock<Object<M>>>, start: usize, limit: Option<usize>) -> Self {
        let end = limit.map(|limit| start.saturating_add(limit));

        Self {
            object,
            end,
            pos: start,
        }
    }
}

impl<const M: usize> AsyncRead for ObjectReader<M> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        let object = match this.object.try_read() {
            Ok(g) => g,
            Err(_) => {
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
        };

        let end = this.end.unwrap_or(object.len).min(object.len);

        if this.pos >= end {
            return std::task::Poll::Ready(Ok(0));
        }

        let idx_chunk = this.pos / M;
        let chunk_pos = this.pos % M;
        let remaining = end - this.pos;
        let chunk_end = (chunk_pos + remaining).min(M);

        let available = &object.chunks[idx_chunk][chunk_pos..chunk_end];
        let n = std::cmp::min(buf.len(), available.len());
        buf[..n].copy_from_slice(&available[..n]);

        this.pos += n;
        std::task::Poll::Ready(Ok(n))
    }
}

struct Object<const M: usize> {
    chunks: Vec<[u8; M]>,
    len: usize,
}

pub struct Memory<const N: usize, const M: usize> {
    storage: Mutex<[Option<[u8; M]>; N]>,
    table: RwLock<BTreeMap<u128, Arc<RwLock<Object<M>>>>>,
}

impl<const N: usize, const M: usize> Default for Memory<N, M> {
    fn default() -> Self {
        Self {
            storage: Mutex::new([Some([0; M]); N]),
            table: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize, const M: usize> Backend for Memory<N, M> {
    async fn write(&self, size: u64, mut reader: BoxedAsyncReader) -> Result<String, BackendError> {
        let mut object = {
            let mut storage = self.storage.lock().await;

            let chunks_needed = size.div_ceil(M as u64) as usize;
            let chunks_idx: Vec<_> = storage
                .iter()
                .enumerate()
                .filter_map(|(idx, &chunk)| chunk.map(|_| idx))
                .take(chunks_needed)
                .collect();

            if chunks_idx.len() < chunks_needed {
                let free_size = (chunks_idx.len() * M) as u64;

                return Err(BackendError::ExceedLimitSize {
                    max: free_size,
                    actual: size,
                });
            }

            let mut object = Object::<M> {
                chunks: Vec::new(),
                len: size as usize,
            };

            for idx in chunks_idx.into_iter() {
                let chunk = storage[idx]
                    .take()
                    .ok_or_else(|| BackendError::Other("unable to take free chunk".into()))?;
                object.chunks.push(chunk);
            }

            object
        };

        {
            let reminder_len = object.len % M;
            let last_idx = object.chunks.len().saturating_sub(1);
            for idx in 0..object.chunks.len() {
                let is_last = idx == last_idx;

                let _ = if is_last && reminder_len != 0 {
                    reader.read_exact(&mut object.chunks[idx][..reminder_len])
                } else {
                    reader.read_exact(&mut object.chunks[idx])
                }
                .await
                .map_err(|e| BackendError::Other(e.into()))?;
            }
        }

        let key = {
            let mut table = self.table.write().await;

            let object = Arc::new(RwLock::new(object));
            let key = uuid::Uuid::new_v4().as_u128();
            let _ = table.insert(key, object);

            key.to_string()
        };

        Ok(key)
    }

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError> {
        let offset = offset as usize;
        let limit = limit.map(|v| v as usize);

        let table = self.table.read().await;

        let key = match key.parse::<u128>() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let object = match table.get(&key) {
            Some(v) => v.clone(),
            None => return Ok(None),
        };

        let reader = ObjectReader::new(object, offset, limit);
        let boxpinreader = Box::pin(reader);

        Ok(Some(boxpinreader))
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let key = match key.parse::<u128>() {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let object = {
            let mut table = self.table.write().await;
            let object = table.remove(&key);
            match object {
                Some(v) => v,
                None => return Ok(()),
            }
        };

        {
            let mut object = object.write().await;
            let mut storage = self.storage.lock().await;
            for idx in 0..storage.len() {
                if storage[idx].is_none() {
                    storage[idx] = object.chunks.pop();
                    if storage[idx].is_none() {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use futures::io::Cursor;

    #[tokio::test]
    async fn test_read_non_existing() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();

        let key = "key".to_string();
        let result = backend.read(key, 0, None).await;

        if !result.is_ok_and(|v| v.is_none()) {
            return Err(anyhow!("should not be able to read non existing object"));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_write_over_free_capacity() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();

        let content = "content";
        let content_reader = Box::pin(Cursor::new(content.as_bytes()));
        let result = backend.write(content.len() as u64, content_reader).await;

        match result {
            Err(BackendError::ExceedLimitSize { max: _, actual: _ }) => (),
            _ => {
                return Err(anyhow!(
                    "shoud spit error input size bigger than the free capacity"
                ));
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_write_then_read() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(content.len() as u64, content_reader).await?;

        let mut reader = backend
            .read(key, 0, None)
            .await?
            .expect("key should be exist after write");

        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;

        assert_eq!(out[..], content);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_then_read_with_offset() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(content.len() as u64, content_reader).await?;

        let mut reader = backend
            .read(key, 1, None)
            .await?
            .expect("key should be exist after write");

        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;

        assert_eq!(out[..], content[1..]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_then_read_with_offset_and_limit() -> anyhow::Result<()> {
        let backend = Memory::<8, 2>::default();

        let content: Vec<u8> = (0..10).collect();
        let content_reader = Box::pin(Cursor::new(content.clone()));

        let key = backend.write(content.len() as u64, content_reader).await?;

        let mut reader = backend
            .read(key, 2, Some(5))
            .await?
            .expect("key should be exist after write");

        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;

        assert_eq!(out[..], content[2..7]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_then_read_with_offset_larger_than_limit() -> anyhow::Result<()> {
        let backend = Memory::<8, 2>::default();

        let content: Vec<u8> = (0..10).collect();
        let content_reader = Box::pin(Cursor::new(content.clone()));

        let key = backend.write(content.len() as u64, content_reader).await?;

        let mut reader = backend
            .read(key, 5, Some(3))
            .await?
            .expect("key should be exist after write");

        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;

        assert_eq!(out[..], content[5..8]);

        Ok(())
    }

    #[tokio::test]
    async fn test_write_zero_size_then_read() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content: [u8; 0] = [];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(0, content_reader).await?;

        let mut reader = backend
            .read(key, 0, None)
            .await?
            .expect("key should be exist after write");

        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;

        assert!(out.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_on_write() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let _ = backend.write(content.len() as u64, content_reader).await?;

        let free_map = {
            let storage = backend.storage.lock().await;
            storage.iter().map(|v| v.is_some()).collect::<Vec<_>>()
        };

        assert_eq!(free_map[..], [false, false, true]);

        Ok(())
    }

    #[tokio::test]
    async fn test_storage_on_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(content.len() as u64, content_reader).await?;
        backend.delete(key).await?;

        let free_map = {
            let storage = backend.storage.lock().await;
            storage.iter().map(|v| v.is_some()).collect::<Vec<_>>()
        };

        assert_eq!(free_map[..], [true, true, true]);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(content.len() as u64, content_reader).await?;
        backend.delete(key.clone()).await?;

        let result = backend.read(key, 0, None).await;

        if !result.is_ok_and(|v| v.is_none()) {
            return Err(anyhow!("should not be able to read non existing object"));
        }

        Ok(())
    }
}
