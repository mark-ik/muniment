//! A redb-backed [`Backend`], the durable desktop store.
//!
//! redb is an embedded key/value store whose two properties the seam wants:
//! native **ordered ranges** (so [`scan`](Backend::scan) is a real range read,
//! not a filtered full listing) and **single-writer transactions** (so
//! [`apply`](Backend::apply) commits a multi-key batch all-or-nothing). Its I/O
//! is synchronous, so the async methods do their redb work inline and return
//! ready futures, which is exactly what the `?Send` seam expects of a desktop
//! backend.
//!
//! Everything lives in one table keyed by muniment's opaque strings. The browser
//! never compiles this module (it is behind the `redb` feature) and reaches for
//! OPFS instead.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
// Leading `::` so this resolves to the external crate, not this module.
use ::redb::{Database, TableDefinition};

use crate::backend::{Backend, WriteOp};
use crate::error::StoreError;

/// The single key/value table: opaque string keys, raw byte values.
const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("muniment");

/// Wrap any redb error as a backend failure, keeping the seam backend-agnostic.
fn backend(err: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(err.to_string())
}

/// A [`Backend`] over a redb embedded database: muniment's durable desktop store.
///
/// Cheap to clone (the database is shared behind an `Arc`), so one handle can
/// seed both a `SlotStore` and a `BlobStore`, the way a filesystem handle is
/// cheap to clone.
#[derive(Clone)]
pub struct RedbBackend {
    db: Arc<Database>,
}

impl RedbBackend {
    /// Open a database at `path`, creating it if absent.
    ///
    /// The table is materialized up front so a read on a fresh database sees an
    /// empty table rather than a "table does not exist" error.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db = Database::create(path).map_err(backend)?;
        let txn = db.begin_write().map_err(backend)?;
        txn.open_table(TABLE).map_err(backend)?;
        txn.commit().map_err(backend)?;
        Ok(Self { db: Arc::new(db) })
    }
}

#[async_trait(?Send)]
impl Backend for RedbBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let read = self.db.begin_read().map_err(backend)?;
        let table = read.open_table(TABLE).map_err(backend)?;
        match table.get(key).map_err(backend)? {
            Some(value) => Ok(Some(value.value().to_vec())),
            None => Ok(None),
        }
    }

    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(TABLE).map_err(backend)?;
            table.insert(key, bytes).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(TABLE).map_err(backend)?;
            table.remove(key).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let read = self.db.begin_read().map_err(backend)?;
        let table = read.open_table(TABLE).map_err(backend)?;
        let mut keys = Vec::new();
        // Range from the prefix and stop at the first key that no longer carries
        // it, so this walks only the matching span, not the whole table.
        for entry in table.range(prefix..).map_err(backend)? {
            let (key, _value) = entry.map_err(backend)?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            keys.push(key.to_string());
        }
        Ok(keys)
    }

    async fn scan(&self, start: &str, end: &str) -> Result<Vec<String>, StoreError> {
        let read = self.db.begin_read().map_err(backend)?;
        let table = read.open_table(TABLE).map_err(backend)?;
        let mut keys = Vec::new();
        // redb ranges are ascending and half-open, matching the seam's contract.
        for entry in table.range(start..end).map_err(backend)? {
            let (key, _value) = entry.map_err(backend)?;
            keys.push(key.value().to_string());
        }
        Ok(keys)
    }

    async fn apply(&self, ops: &[WriteOp]) -> Result<(), StoreError> {
        let txn = self.db.begin_write().map_err(backend)?;
        {
            let mut table = txn.open_table(TABLE).map_err(backend)?;
            for op in ops {
                match op {
                    WriteOp::Put { key, value } => {
                        table.insert(key.as_str(), value.as_slice()).map_err(backend)?;
                    }
                    WriteOp::Delete { key } => {
                        table.remove(key.as_str()).map_err(backend)?;
                    }
                }
            }
        }
        // One commit for the whole batch: it lands atomically, or not at all.
        txn.commit().map_err(backend)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend over a fresh database in a scratch directory, plus the dir
    /// (kept alive so the file is not removed mid-test).
    fn temp_backend() -> (tempfile::TempDir, RedbBackend) {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(dir.path().join("store.redb")).unwrap();
        (dir, backend)
    }

    #[test]
    fn put_get_delete_round_trip() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            assert_eq!(b.get("k").await.unwrap(), None);
            b.put("k", b"v").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), Some(b"v".to_vec()));
            // Overwrite.
            b.put("k", b"v2").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), Some(b"v2".to_vec()));
            b.delete("k").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), None);
            // Deleting an absent key is not an error.
            b.delete("k").await.unwrap();
        });
    }

    #[test]
    fn list_returns_only_the_prefix() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            for k in ["blob/a", "blob/b", "slot/x"] {
                b.put(k, b"v").await.unwrap();
            }
            let mut blobs = b.list("blob/").await.unwrap();
            blobs.sort();
            assert_eq!(blobs, vec!["blob/a".to_string(), "blob/b".to_string()]);
            assert_eq!(b.list("slot/").await.unwrap(), vec!["slot/x".to_string()]);
            assert!(b.list("none/").await.unwrap().is_empty());
        });
    }

    #[test]
    fn scan_returns_the_range_in_ascending_order() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            // Two logs, entries inserted out of order.
            for (k, v) in [
                ("log/a/0/0000000000000002", "two"),
                ("log/a/0/0000000000000000", "zero"),
                ("log/a/0/0000000000000001", "one"),
                ("log/b/0/0000000000000000", "other-log"),
            ] {
                b.put(k, v.as_bytes()).await.unwrap();
            }
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
            let (_dir, b) = temp_backend();
            b.put("keep", b"v").await.unwrap();
            b.put("drop", b"v").await.unwrap();
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
    fn state_survives_reopen() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.redb");
            {
                let b = RedbBackend::open(&path).unwrap();
                b.put("k", b"durable").await.unwrap();
            }
            // A fresh handle on the same file sees the committed write.
            let reopened = RedbBackend::open(&path).unwrap();
            assert_eq!(reopened.get("k").await.unwrap(), Some(b"durable".to_vec()));
        });
    }
}
