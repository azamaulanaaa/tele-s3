use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use super::{Backend, BackendError, BoxedAsyncReader};

struct Chunk<const M: usize> {
    data: [u8; M],
}

impl<const M: usize> Default for Chunk<M> {
    fn default() -> Self {
        Self { data: [0; M] }
    }
}

struct ObjectMetadata {
    chunks: Vec<usize>,
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
    async fn write(&self, size: u64, reader: BoxedAsyncReader) -> Result<String, BackendError> {
        let free_map = self.free_map.read().await;

        let free_size = (free_map.iter().filter(|&&is_free| is_free).count() * M) as u64;
        if size > free_size {
            return Err(BackendError::ExceedLimitSize {
                max: free_size,
                actual: size,
            });
        }

        todo!()
    }

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError> {
        let table = self.table.read().await;

        let key = match key.parse() {
            Ok(v) => v,
            Err(_) => return Ok(None),
        };

        let metadata = match table.get(&key) {
            Some(v) => v,
            None => return Ok(None),
        };

        todo!()
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        todo!()
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
}
