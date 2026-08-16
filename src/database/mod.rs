pub mod inner;
use crate::config::LsmliteConfig;
use crate::memtable::inner::Blob;
use crate::restart::{SSTableArtifact, WalArtifact};
use inner::LsmliteInner;
use std::sync::Arc;

/// Database wrapper. Only exposes the three supported statements, GET, INSERT (write), and DELETE.
/// TODO: add auth public api.
#[derive(Clone, Debug)]
pub struct LsmLite {
    inner: Arc<LsmliteInner>,
}

impl LsmLite {
    pub fn new(
        sstable_files: Vec<SSTableArtifact>,
        wal_files: Vec<WalArtifact>,
        config: &LsmliteConfig,
    ) -> LsmLite {
        let inner = Arc::new(LsmliteInner::new(sstable_files, wal_files, config));

        LsmLite { inner }
    }

    pub async fn set(&self, key: Blob, value: Blob) {
        self.inner.set(key, value).await
    }

    pub async fn get(&self, key: &[u8]) -> Option<Blob> {
        self.inner.get(key).await
    }

    pub async fn delete(&self, key: Blob) {
        self.inner.delete(key).await
    }
}
