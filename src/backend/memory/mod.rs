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

struct ObjectMetadata<const N: usize> {
    chunks: Vec<usize>,
    len: usize,
}

pub struct Memory<const N: usize, const M: usize> {
    storage: Box<[Arc<RwLock<Chunk<M>>>; N]>,
    free_map: RwLock<[bool; N]>,
    table: RwLock<BTreeMap<u64, ObjectMetadata<N>>>,
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
        todo!()
    }

    async fn read(
        &self,
        key: String,
        offset: u64,
        limit: Option<u64>,
    ) -> Result<Option<BoxedAsyncReader>, BackendError> {
        todo!()
    }

    async fn delete(&self, key: String) -> Result<(), BackendError> {
        todo!()
    }
}
