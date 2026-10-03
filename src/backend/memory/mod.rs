use std::collections::BTreeMap;
use std::sync::Arc;

use futures::{AsyncRead, AsyncReadExt};
use tokio::sync::{Mutex, RwLock};

use super::{Backend, BackendError, BoxedAsyncReader, ReadOutcome, ReclaimOutcome, is_publish_id};

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
    table: RwLock<BTreeMap<String, Arc<RwLock<Object<M>>>>>,
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
    /// Store the bytes under the caller-supplied `publish_id` and return it
    /// unchanged as the key.
    ///
    /// The identifier is opaque and chosen by the layer above so it can be
    /// recorded as a publish intent before this call and used to find the
    /// object afterwards. This backend honours the contract literally: the
    /// key is the identifier it was handed, byte for byte.
    async fn write(
        &self,
        publish_id: String,
        size: u64,
        mut reader: BoxedAsyncReader,
    ) -> Result<String, BackendError> {
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
                // `chunks_idx` was collected from this very array under
                // this very lock, so a slot that was free a moment ago
                // cannot be occupied now. Reaching `None` means the
                // backend's own bookkeeping is corrupt, which is neither
                // recoverable by the caller nor retryable. The `detail`
                // stays out of the client's response, so it is logged here.
                let chunk = storage[idx].take().ok_or_else(|| {
                    tracing::error!(
                        chunk_index = idx,
                        "backend write fault: a free chunk slot was no longer free"
                    );
                    BackendError::InvariantViolation {
                        detail: "a free chunk slot was no longer free",
                    }
                })?;
                object.chunks.push(chunk);
            }

            object
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
                // The body ended early: the chunks taken from storage were
                // never published to the table, so hand them back instead
                // of leaking capacity on every short write.
                let mut storage = self.storage.lock().await;
                for chunk in object.chunks {
                    match storage.iter_mut().find(|slot| slot.is_none()) {
                        Some(slot) => *slot = Some(chunk),
                        None => break,
                    }
                }

                return Err(BackendError::Other(e.into()));
            }
        }

        let key = {
            let mut table = self.table.write().await;

            let object = Arc::new(RwLock::new(object));
            // A collision would make two objects share one entry, so the
            // caller-owned identifier must be unique; the S3 layer mints it
            // from a UUID for exactly this reason.
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

        let table = self.table.read().await;

        // A key that is not the shape of an identifier this backend was
        // handed names nothing it ever wrote, so the object is not absent —
        // it is unreadable. Same rule `Grammers` applies, and deliberately
        // opposite to `delete`, where an unaddressable key is an absent key
        // by construction.
        if !is_publish_id(&key) {
            // Logged here, where the offending key is still in hand: the
            // fault's `detail` never reaches the client.
            tracing::error!(key = %key, "backend read fault: key is not a key this backend minted");
            return Ok(ReadOutcome::Fault(BackendError::InvariantViolation {
                detail: "key is not a key this backend minted",
            }));
        }

        let object = match table.get(&key) {
            Some(v) => v.clone(),
            None => return Ok(ReadOutcome::Missing),
        };

        let reader = ObjectReader::new(object, offset, limit);
        let boxpinreader = Box::pin(reader);

        Ok(ReadOutcome::Present(boxpinreader))
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        // Idempotent by contract: a key that names nothing — including one
        // this backend cannot parse, which names nothing it ever wrote — is
        // deleted successfully.
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

    async fn reclaim_publish(&self, publish_id: String) -> Result<ReclaimOutcome, BackendError> {
        // Always "nothing published", and that is a fact about this backend
        // rather than a shortcut past one. Its table lives inside this
        // process and dies with it, so a write whose answer was lost cannot
        // leave anything a later pass could find: the pass necessarily runs
        // in a *later* process, by which time the table is gone. Within one
        // process the table is also written only on the success path of
        // `write`, so a pending intent has no entry under this identifier
        // either.
        let _ = publish_id;

        Ok(ReclaimOutcome::NothingPublished)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::anyhow;
    use futures::io::Cursor;

    /// Assert that a `read` found the object and hand back its reader.
    fn present(outcome: ReadOutcome) -> anyhow::Result<BoxedAsyncReader> {
        match outcome {
            ReadOutcome::Present(reader) => Ok(reader),
            other => Err(anyhow!("expected a present object, got {other:?}")),
        }
    }

    /// A fresh identifier, in the shape the S3 layer mints them.
    fn publish_id() -> String {
        crate::backend::new_publish_id()
    }

    #[tokio::test]
    async fn test_read_non_existing() -> anyhow::Result<()> {
        let backend = Memory::<1, 1>::default();

        // A well-formed identifier that was never written: genuinely
        // missing, as opposed to a key this backend cannot address at all.
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

        // Never written, and twice, plus once under a key this backend
        // cannot address at all: the idempotency contract covers all of them.
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

        // The contract the whole publish-intent scheme rests on: the key the
        // backend returns is the identifier it was given, verbatim, so the
        // caller can find the object by what it recorded beforehand.
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

        // And the object really is addressable by that identifier.
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
            let storage = backend.storage.lock().await;
            storage.iter().map(|v| v.is_some()).collect::<Vec<_>>()
        };

        assert_eq!(free_map[..], [false, false, true]);

        Ok(())
    }

    #[tokio::test]
    async fn reclaim_reports_nothing_published() -> anyhow::Result<()> {
        let backend = Memory::<3, 2>::default();

        // An identifier this backend was never handed, and one it was handed
        // and published: both answer the same, because its table dies with the
        // process and so can never hold anything a later pass could find.
        let published = backend
            .write(publish_id(), 3, Box::pin(Cursor::new([1, 2, 3])))
            .await?;
        assert!(!published.is_empty());

        for id in [publish_id(), published] {
            let outcome = backend.reclaim_publish(id.clone()).await?;
            assert_eq!(
                outcome,
                ReclaimOutcome::NothingPublished,
                "{id} has no external store to reclaim from"
            );
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
}
