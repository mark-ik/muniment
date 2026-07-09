//! The host-supplied storage backend seam, plus an in-memory backend.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::error::StoreError;

/// A single write in an [`apply`](Backend::apply) batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOp {
    /// Write `value` at `key`, overwriting any previous value.
    Put {
        /// The key to write.
        key: String,
        /// The bytes to store.
        value: Vec<u8>,
    },
    /// Remove `key`. Absent keys are not an error.
    Delete {
        /// The key to remove.
        key: String,
    },
}

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

    /// Keys in the half-open lexicographic range `[start, end)`, in **ascending
    /// order**. This is the ordered read a log needs: a per-author log lives at
    /// contiguous fixed-width keys, so `[lo, hi)` returns its entries in `seq`
    /// order. The default filters and sorts [`list`](Backend::list); a backend
    /// with a native ordered index (redb ranges, IndexedDB cursors) overrides it.
    async fn scan(&self, start: &str, end: &str) -> Result<Vec<String>, StoreError> {
        let mut keys: Vec<String> = self
            .list("")
            .await?
            .into_iter()
            .filter(|k| k.as_str() >= start && k.as_str() < end)
            .collect();
        keys.sort();
        Ok(keys)
    }

    /// Apply a batch of writes **atomically**: every op lands, or none does.
    ///
    /// One store write can touch several keys (an operation's payload blob plus
    /// its log-index entry), and a reader must never see half of it. The default
    /// applies the ops in order — safe when the writes are content-addressed or
    /// idempotent, so a crash mid-batch leaves only recoverable orphans — and a
    /// transactional backend (redb, an IndexedDB object-store transaction)
    /// overrides it with a real all-or-nothing commit. The whole batch arrives in
    /// one call, so a backend issues every write in a single transaction / tick
    /// with no caller-controlled await between ops.
    async fn apply(&self, ops: &[WriteOp]) -> Result<(), StoreError> {
        for op in ops {
            match op {
                WriteOp::Put { key, value } => self.put(key, value).await?,
                WriteOp::Delete { key } => self.delete(key).await?,
            }
        }
        Ok(())
    }
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

    async fn scan(&self, start: &str, end: &str) -> Result<Vec<String>, StoreError> {
        let mut keys: Vec<String> = self
            .map
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.as_str() >= start && k.as_str() < end)
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    /// Atomic under the single mutex: the whole batch applies while the lock is
    /// held, so no reader observes a partial batch.
    async fn apply(&self, ops: &[WriteOp]) -> Result<(), StoreError> {
        let mut map = self.map.lock().unwrap();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    map.insert(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    map.remove(key);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend seeded with two logs, entries inserted out of order.
    fn seed() -> MemoryBackend {
        let b = MemoryBackend::new();
        pollster::block_on(async {
            for (k, v) in [
                ("log/a/0/0000000000000002", "two"),
                ("log/a/0/0000000000000000", "zero"),
                ("log/a/0/0000000000000001", "one"),
                ("log/b/0/0000000000000000", "other-log"),
            ] {
                b.put(k, v.as_bytes()).await.unwrap();
            }
        });
        b
    }

    #[test]
    fn scan_returns_the_range_in_ascending_order() {
        pollster::block_on(async {
            let b = seed();
            // Log a/0, entries 0..3 in seq order despite insertion order; the
            // other log is excluded by the range.
            let keys = b
                .scan("log/a/0/0000000000000000", "log/a/0/0000000000000003")
                .await
                .unwrap();
            assert_eq!(
                keys,
                vec![
                    "log/a/0/0000000000000000".to_string(),
                    "log/a/0/0000000000000001".to_string(),
                    "log/a/0/0000000000000002".to_string(),
                ]
            );
            // Half-open: [1, 2) is exactly entry 1.
            let one = b
                .scan("log/a/0/0000000000000001", "log/a/0/0000000000000002")
                .await
                .unwrap();
            assert_eq!(one, vec!["log/a/0/0000000000000001".to_string()]);
        });
    }

    #[test]
    fn apply_lands_the_whole_batch() {
        pollster::block_on(async {
            let b = MemoryBackend::new();
            b.put("keep", b"v").await.unwrap();
            b.put("drop", b"v").await.unwrap();
            // One batch: an op header + its payload blob + a delete, all together.
            b.apply(&[
                WriteOp::Put {
                    key: "op/h".into(),
                    value: b"header".to_vec(),
                },
                WriteOp::Put {
                    key: "op/h/payload".into(),
                    value: b"body".to_vec(),
                },
                WriteOp::Delete { key: "drop".into() },
            ])
            .await
            .unwrap();
            assert_eq!(b.get("op/h").await.unwrap(), Some(b"header".to_vec()));
            assert_eq!(b.get("op/h/payload").await.unwrap(), Some(b"body".to_vec()));
            assert_eq!(b.get("drop").await.unwrap(), None);
            assert_eq!(b.get("keep").await.unwrap(), Some(b"v".to_vec()));
        });
    }

    #[test]
    fn apply_takes_the_whole_batch_in_one_call() {
        // The seam-level batch-spanning-await guard: `apply` receives every write
        // at once, so a transactional backend commits them in one transaction with
        // no caller-controlled await between ops. Assert the shape: an empty batch
        // is a no-op; a batch is applied wholesale.
        pollster::block_on(async {
            let b = MemoryBackend::new();
            b.apply(&[]).await.unwrap();
            assert!(b.is_empty(), "empty batch writes nothing");
            b.apply(&[WriteOp::Put {
                key: "k".into(),
                value: b"v".to_vec(),
            }])
            .await
            .unwrap();
            assert_eq!(b.len(), 1);
        });
    }
}
