//! The host-supplied storage backend seam, plus an in-memory backend.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::StoreError;

/// A host-supplied key/value byte store. The host realizes it as the filesystem
/// on desktop, OPFS in the browser, or an embedded store (redb, fjall). muniment
/// defines the contract; it never picks a backend.
///
/// `?Send` on purpose: browser OPFS on the main thread can only await JS
/// promises, whose futures are not `Send`. Desktop and embedded backends do
/// their I/O synchronously and return ready futures, so this bound costs them
/// nothing. Consumers `.await` in whatever context they already have.
///
/// Keys are opaque strings. muniment's stores namespace them (`blob/<hash>`,
/// a consumer's own slot names); a backend treats them as flat byte keys.
#[async_trait(?Send)]
pub trait Backend {
    /// The bytes at `key`, or `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;

    /// Write `bytes` at `key`, overwriting any previous value.
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError>;

    /// Remove `key`. Absent keys are not an error.
    async fn delete(&self, key: &str) -> Result<(), StoreError>;

    /// Every key beginning with `prefix`, in unspecified order.
    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

/// An in-memory [`Backend`], the deterministic test and development floor. Not
/// durable: state lives only as long as the handle. Cheap to clone (a shared
/// handle), so one instance can seed both a `SlotStore` and a `BlobStore`, the
/// way a real filesystem or OPFS handle is cheap to clone.
#[derive(Clone, Default)]
pub struct MemoryBackend {
    map: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MemoryBackend {
    /// A fresh, empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of stored keys.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Whether the backend holds no keys.
    pub fn is_empty(&self) -> bool {
        self.map.lock().unwrap().is_empty()
    }
}

#[async_trait(?Send)]
impl Backend for MemoryBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        self.map.lock().unwrap().insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        Ok(self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}
