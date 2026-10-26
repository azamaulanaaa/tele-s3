use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use futures::{AsyncRead, AsyncReadExt};
use tokio::sync::RwLock;

use super::{Backend, BackendError, BoxedAsyncReader, ReadOutcome, ReclaimOutcome, is_publish_id};

/// Number of shards for the in-memory object table.
///
/// A small fixed count keeps lock contention low without turning every
/// lookup into a long shard walk.
const TABLE_SHARDS: usize = 16;

struct Object<const M: usize> {
    chunks: Vec<[u8; M]>,
    slots: Vec<usize>,
    len: usize,
}

struct ObjectReader<const M: usize> {
    object: Arc<Object<M>>,
    end: Option<usize>,
    pos: usize,
}

impl<const M: usize> ObjectReader<M> {
    /// `limit` is a byte count relative to `start`; it is converted to an
    /// absolute end position so remaining-length math can never underflow.
    fn new(object: Arc<Object<M>>, start: usize, limit: Option<usize>) -> Self {
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
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        let end = this.end.unwrap_or(this.object.len).min(this.object.len);

        if this.pos >= end {
            return std::task::Poll::Ready(Ok(0));
        }

        let idx_chunk = this.pos / M;
        let chunk_pos = this.pos % M;
        let remaining = end - this.pos;
        let chunk_end = (chunk_pos + remaining).min(M);

        let available = &this.object.chunks[idx_chunk][chunk_pos..chunk_end];
        let n = std::cmp::min(buf.len(), available.len());
        buf[..n].copy_from_slice(&available[..n]);

        this.pos += n;
        std::task::Poll::Ready(Ok(n))
    }
}

pub struct Memory<const N: usize, const M: usize> {
    storage: Vec<StdMutex<Option<[u8; M]>>>,
    table: Vec<RwLock<BTreeMap<String, Arc<Object<M>>>>>,
}

impl<const N: usize, const M: usize> Default for Memory<N, M> {
    fn default() -> Self {
        let mut storage = Vec::with_capacity(N);
        for _ in 0..N {
            storage.push(StdMutex::new(Some([0; M])));
        }

        let mut table = Vec::with_capacity(TABLE_SHARDS);
        for _ in 0..TABLE_SHARDS {
            table.push(RwLock::new(BTreeMap::new()));
        }

        Self { storage, table }
    }
}

impl<const N: usize, const M: usize> Memory<N, M> {
    fn shard_for(key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % TABLE_SHARDS
    }
}

#[async_trait::async_trait]
impl<const N: usize, const M: usize> Backend for Memory<N, M> {
    async fn write(
        &self,
        publish_id: String,
        size: u64,
        mut reader: BoxedAsyncReader,
    ) -> Result<String, BackendError> {
        let chunks_needed = size.div_ceil(M as u64) as usize;

        // Allocate free slots, locking each slot only briefly.
        let mut allocated: Vec<(usize, [u8; M])> = Vec::with_capacity(chunks_needed);
        for (idx, slot) in self.storage.iter().enumerate() {
            if allocated.len() >= chunks_needed {
                break;
            }
            let mut guard = slot.lock().map_err(|_| BackendError::PoisonedLock {
                lock: "memory storage slot",
            })?;
            if let Some(chunk) = guard.take() {
                allocated.push((idx, chunk));
            }
        }

        if allocated.len() < chunks_needed {
            let free_count = allocated.len();
            for (idx, chunk) in allocated {
                let mut guard =
                    self.storage[idx]
                        .lock()
                        .map_err(|_| BackendError::PoisonedLock {
                            lock: "memory storage slot",
                        })?;
                *guard = Some(chunk);
            }
            return Err(BackendError::ExceedLimitSize {
                max: (free_count * M) as u64,
                actual: size,
            });
        }

        let chunks_idx: Vec<usize> = allocated.iter().map(|(idx, _)| *idx).collect();
        let chunks_data: Vec<[u8; M]> = allocated.into_iter().map(|(_, chunk)| chunk).collect();

        let mut object = Object::<M> {
            chunks: chunks_data,
            slots: chunks_idx,
            len: size as usize,
        };

        {
            let reminder_len = object.len % M;
            let last_idx = object.chunks.len().saturating_sub(1);
            let mut failure: Option<std::io::Error> = None;
            for idx in 0..object.chunks.len() {
                let is_last = idx == last_idx;

                let res = if is_last && reminder_len != 0 {
                    reader.read_exact(&mut object.chunks[idx][..reminder_len])
                } else {
                    reader.read_exact(&mut object.chunks[idx])
                }
                .await;

                if let Err(e) = res {
                    failure = Some(e);
                    break;
                }
            }

            if let Some(e) = failure {
                // Hand capacity back to the original slots.
                for (chunk, slot_idx) in object.chunks.into_iter().zip(object.slots.iter()) {
                    let mut guard =
                        self.storage[*slot_idx]
                            .lock()
                            .map_err(|_| BackendError::PoisonedLock {
                                lock: "memory storage slot",
                            })?;
                    *guard = Some(chunk);
                }

                return Err(BackendError::Other(e.into()));
            }
        }

        tracing::debug!(
            publish_id = %publish_id,
            bytes = size,
            "memory backend storing object"
        );

        let key = {
            let shard = Self::shard_for(&publish_id);
            let mut table = self.table[shard].write().await;

            let object = Arc::new(object);
            let _ = table.insert(publish_id.clone(), object);

            publish_id
        };

        Ok(key)
    }

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<ReadOutcome, BackendError> {
        let offset = offset as usize;
        let limit = limit.map(|v| v as usize);

        if !is_publish_id(&key) {
            tracing::error!(
                key = %key,
                error.type = "key_not_minted",
                "backend read fault: key is not a key this backend minted"
            );
            return Ok(ReadOutcome::Fault(BackendError::InvariantViolation {
                detail: "key is not a key this backend minted",
            }));
        }

        tracing::debug!(key = %key, offset, limit = ?limit, "memory backend read");

        let shard = Self::shard_for(&key);
        let table = self.table[shard].read().await;

        let object = match table.get(&key) {
            Some(v) => v.clone(),
            None => return Ok(ReadOutcome::Missing),
        };

        let reader = ObjectReader::new(object, offset, limit);
        let boxpinreader = Box::pin(reader);

        Ok(ReadOutcome::Present(boxpinreader))
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        let object = {
            let shard = Self::shard_for(&key);
            let mut table = self.table[shard].write().await;
            table.remove(&key)
        };

        let object = match object {
            Some(v) => v,
            None => return Ok(()),
        };

        // Reclaim capacity back to original slots. Readers keep their own
        // immutable copy of the chunks, so copying here is safe.
        for (chunk, slot_idx) in object.chunks.iter().zip(object.slots.iter()) {
            let mut guard =
                self.storage[*slot_idx]
                    .lock()
                    .map_err(|_| BackendError::PoisonedLock {
                        lock: "memory storage slot",
                    })?;
            *guard = Some(*chunk);
        }

        Ok(())
    }

    async fn reclaim_publish(&self, publish_id: String) -> Result<ReclaimOutcome, BackendError> {
        let _ = publish_id;
        Ok(ReclaimOutcome::NothingPublished)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use futures::io::Cursor;

    use crate::backend::grammers::capture_log;

    /// A stored object is announced at `debug` with its size.
    #[test]
    fn a_write_is_announced_with_its_publish_id_and_size() {
        let log = capture_log(|| {
            futures::executor::block_on(async {
                let backend = Memory::<2, 8>::default();
                let body = Box::pin(Cursor::new(vec![1u8, 2, 3]));
                backend
                    .write(publish_id(), 3, body)
                    .await
                    .expect("a 3-byte body fits");
            })
        });

        assert!(log.contains("publish_id="), "{log}");
        assert!(log.contains("bytes=3"), "{log}");
        assert!(log.contains("DEBUG"), "{log}");
    }

    /// A read is announced at `debug` with the range it was asked for.
    #[test]
    fn a_read_is_announced_with_its_range() {
        let log = capture_log(|| {
            futures::executor::block_on(async {
                let backend = Memory::<3, 2>::default();
                let key = backend
                    .write(publish_id(), 3, Box::pin(Cursor::new(vec![1u8, 2, 3])))
                    .await
                    .expect("the key is readable");
                backend
                    .read(key, 1, None)
                    .await
                    .expect("the key is readable");
            })
        });

        assert!(log.contains("key="), "{log}");
        assert!(log.contains("offset=1"), "{log}");
        assert!(log.contains("DEBUG"), "{log}");
    }

    /// An unaddressable key is a fault, not an absence.
    #[test]
    fn an_unparseable_key_is_logged_as_its_own_error_type() {
        let log = capture_log(|| {
            futures::executor::block_on(async {
                let backend = Memory::<1, 1>::default();
                let _ = backend.read("not-a-key".to_owned(), 0, None).await;
            })
        });

        assert!(log.contains("key=not-a-key"), "{log}");
        assert!(log.contains("error.type"), "{log}");
        assert!(log.contains("key_not_minted"), "{log}");
    }

    fn present(outcome: ReadOutcome) -> anyhow::Result<BoxedAsyncReader> {
        match outcome {
            ReadOutcome::Present(reader) => Ok(reader),
            other => Err(anyhow!("expected a present object, got {other:?}")),
        }
    }

    fn publish_id() -> String {
        crate::backend::new_publish_id()
    }

    #[tokio::test]
    async fn test_read_non_existing() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();
        let key = publish_id();
        let result = backend.read(key, 0, None).await;
        if !result.is_ok_and(|v| matches!(v, ReadOutcome::Missing)) {
            return Err(anyhow!("should report an absent key as missing"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn an_unparseable_key_is_a_fault_and_not_an_absence() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();
        let result = backend.read("key".to_string(), 0, None).await;
        match result {
            Ok(ReadOutcome::Fault(BackendError::InvariantViolation { detail })) => {
                assert_eq!(detail, "key is not a key this backend minted");
                Ok(())
            }
            other => Err(anyhow!("expected a fault, got {other:?}")),
        }
    }

    #[tokio::test]
    async fn delete_of_an_absent_key_succeeds() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        backend.delete(publish_id()).await?;
        backend.delete(publish_id()).await?;
        backend.delete("key".to_string()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_write_over_free_capacity() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();
        let content = "content";
        let content_reader = Box::pin(Cursor::new(content.as_bytes()));
        let result = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await;
        match result {
            Err(BackendError::ExceedLimitSize { max: _, actual: _ }) => (),
            _ => {
                return Err(anyhow!("should reject input larger than free capacity"));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_write_then_read() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        let mut reader = present(backend.read(key, 0, None).await?)?;
        let mut out = Vec::new();
        let _ = reader.read_to_end(&mut out).await?;
        assert_eq!(out[..], content);
        Ok(())
    }

    #[tokio::test]
    async fn the_stored_key_is_the_caller_supplied_identifier() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let id = publish_id();
        let content = [1, 2, 3];
        let key = backend
            .write(
                id.clone(),
                content.len() as u64,
                Box::pin(Cursor::new(content)),
            )
            .await?;
        assert_eq!(key, id);
        let mut reader = present(backend.read(key, 0, None).await?)?;
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
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        let mut reader = present(backend.read(key, 1, None).await?)?;
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
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        let mut reader = present(backend.read(key, 2, Some(5)).await?)?;
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
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        let mut reader = present(backend.read(key, 5, Some(3)).await?)?;
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
        let key = backend.write(publish_id(), 0, content_reader).await?;
        let mut reader = present(backend.read(key, 0, None).await?)?;
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
        let _ = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        let free_map = {
            backend
                .storage
                .iter()
                .map(|slot| slot.lock().unwrap().is_some())
                .collect::<Vec<_>>()
        };
        // After write of 2 chunks, 2 slots occupied, 1 free.
        assert_eq!(free_map.iter().filter(|v| !**v).count(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn reclaim_reports_nothing_published() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let published = backend
            .write(publish_id(), 3, Box::pin(Cursor::new([1, 2, 3])))
            .await?;
        assert!(!published.is_empty());
        for id in [publish_id(), published] {
            let outcome = backend.reclaim_publish(id.clone()).await?;
            assert_eq!(outcome, ReclaimOutcome::NothingPublished);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_storage_on_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        backend.delete(key.clone()).await?;
        let free_map = {
            backend
                .storage
                .iter()
                .map(|slot| slot.lock().unwrap().is_some())
                .collect::<Vec<_>>()
        };
        assert!(free_map.iter().all(|v| *v));
        Ok(())
    }

    #[tokio::test]
    async fn test_read_after_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let content = [1, 2, 3];
        let content_reader = Box::pin(Cursor::new(content));
        let key = backend
            .write(publish_id(), content.len() as u64, content_reader)
            .await?;
        backend.delete(key.clone()).await?;
        let result = backend.read(key, 0, None).await;
        if !result.is_ok_and(|v| matches!(v, ReadOutcome::Missing)) {
            return Err(anyhow!("should report a deleted key as missing"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_reads_complete_after_delete() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();
        let content = [1, 2, 3, 4];
        let key = backend
            .write(
                publish_id(),
                content.len() as u64,
                Box::pin(Cursor::new(content)),
            )
            .await?;
        let reader = present(backend.read(key.clone(), 0, None).await?)?;
        backend.delete(key).await?;
        let mut out = Vec::new();
        // Reader should still complete from its immutable snapshot.
        let mut reader = reader;
        let _ = reader.read_to_end(&mut out).await?;
        assert_eq!(out[..], content);
        Ok(())
    }
}
