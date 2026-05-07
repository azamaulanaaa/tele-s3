use std::sync::Arc;
use std::time::UNIX_EPOCH;
use std::{collections::BTreeMap, time::SystemTime};

use futures::io::Cursor;
use futures::{AsyncRead, AsyncReadExt};
use tokio::sync::RwLock;

use super::{Backend, BackendError, BoxedAsyncReader, ChainReaders};

struct Chunk<const M: usize> {
    data: [u8; M],
}

struct ChunkReader<const M: usize> {
    chunk: Arc<RwLock<Chunk<M>>>,
    start: usize,
    end: usize,
    pos: usize,
}

impl<const M: usize> ChunkReader<M> {
    fn new(chunk: Arc<RwLock<Chunk<M>>>, start: usize, end: usize) -> Self {
        Self {
            chunk,
            start,
            end,
            pos: 0,
        }
    }
}

impl<const M: usize> AsyncRead for ChunkReader<M> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        let guard = match this.chunk.try_read() {
            Ok(g) => g,
            Err(_) => {
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
        };

        let available = &guard.data[this.start + this.pos..this.end];
        let n = std::cmp::min(buf.len(), available.len());
        buf[..n].copy_from_slice(&available[..n]);

        this.pos += n;
        std::task::Poll::Ready(Ok(n))
    }
}

impl<const M: usize> Default for Chunk<M> {
    fn default() -> Self {
        Self { data: [0; M] }
    }
}

struct ObjectMetadata {
    chunks_idx: Vec<usize>,
    len: usize,
}

pub struct Memory<const N: usize, const M: usize> {
    storage: Box<[Arc<RwLock<Chunk<M>>>; N]>,
    free_map: RwLock<[bool; N]>,
    table: RwLock<BTreeMap<u64, ObjectMetadata>>,
}

impl<const N: usize, const M: usize> Default for Memory<N, M> {
    fn default() -> Self {
        let storage = std::array::from_fn(|_| Arc::new(RwLock::new(Chunk::default())));

        Self {
            storage: Box::new(storage),
            free_map: RwLock::new([true; N]),
            table: Default::default(),
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize, const M: usize> Backend for Memory<N, M> {
    async fn write(&self, size: u64, mut reader: BoxedAsyncReader) -> Result<String, BackendError> {
        let metadata = {
            let mut free_map = self.free_map.write().await;

            let chunks_needed = size.div_ceil(M as u64) as usize;
            let chunks_idx: Vec<_> = free_map
                .iter()
                .enumerate()
                .filter_map(|(idx, &is_free)| (is_free).then_some(idx))
                .take(chunks_needed)
                .collect();

            if chunks_idx.len() < chunks_needed {
                let free_size = (chunks_idx.len() * M) as u64;

                return Err(BackendError::ExceedLimitSize {
                    max: free_size,
                    actual: size,
                });
            }

            let metadata = ObjectMetadata {
                chunks_idx,
                len: size as usize,
            };

            for &idx in &metadata.chunks_idx {
                free_map[idx] = false;
            }

            metadata
        };

        let reminder_len = metadata.len % M;

        for (idx, &chunk_idx) in metadata.chunks_idx.iter().enumerate() {
            let chunk = self.storage.get(chunk_idx).ok_or_else(|| {
                BackendError::Other("chunk index logicly should not ever execeed N".into())
            })?;
            let mut chunk = chunk.write().await;

            let is_last = idx == metadata.chunks_idx.len() - 1;

            let _ = if is_last && reminder_len != 0 {
                reader.read_exact(&mut chunk.data[..reminder_len])
            } else {
                reader.read_exact(&mut chunk.data)
            }
            .await
            .map_err(|e| BackendError::Other(e.into()))?;
        }

        let key = {
            let mut table = self.table.write().await;

            let key = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| BackendError::Other(e.into()))?
                .as_secs();

            let _ = table.insert(key, metadata);

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

        let key = match key.parse() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let metadata = match table.get(&key) {
            Some(v) => v,
            None => return Ok(None),
        };

        if offset >= metadata.len {
            return Ok(Some(Box::pin(Cursor::new(vec![]))));
        }

        let available_after_offset = metadata.len - offset;
        let read_len = limit.map_or(available_after_offset, |l| l.min(available_after_offset));

        if read_len == 0 {
            return Ok(Some(Box::pin(Cursor::new(vec![]))));
        }

        let n_skip_chunks = offset / M;
        let start_first_chunk = offset % M;
        let n_chunks = (read_len + start_first_chunk).div_ceil(M);
        let end_last_chunk = (read_len + start_first_chunk) % M;

        let mut readers: Vec<BoxedAsyncReader> = Vec::new();

        for (idx, &chunk_idx) in metadata
            .chunks_idx
            .iter()
            .skip(n_skip_chunks)
            .take(n_chunks)
            .enumerate()
        {
            let mut start = 0;
            let mut end = M;
            if idx == 0 {
                start = start_first_chunk;
            }
            if idx == n_chunks - 1 {
                end = end_last_chunk;
            }

            let chunk_arc = self
                .storage
                .get(chunk_idx)
                .cloned()
                .ok_or_else(|| BackendError::Other("Chunk index data out of bound".into()))?;
            readers.push(Box::pin(ChunkReader::new(chunk_arc, start, end)));
        }

        let readers = Box::pin(ChainReaders::from_vec(readers));

        Ok(Some(readers))
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let key = match key.parse() {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let metadata = {
            let mut table = self.table.write().await;
            let metadata = table.remove(&key);
            match metadata {
                Some(v) => v,
                None => return Ok(()),
            }
        };

        let mut free_map = self.free_map.write().await;
        for chunk_index in metadata.chunks_idx {
            free_map[chunk_index] = true
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
    async fn test_free_map_on_write() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let _ = backend.write(content.len() as u64, content_reader).await?;

        let free_map = backend.free_map.read().await;

        assert_eq!(free_map[..], [false, false, true]);

        Ok(())
    }

    #[tokio::test]
    async fn test_free_map_on_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));

        let key = backend.write(content.len() as u64, content_reader).await?;
        backend.delete(key).await?;

        let free_map = backend.free_map.read().await;

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
