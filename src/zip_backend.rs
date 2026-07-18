//! A zip-archive [`Backend`], a portable and inspectable durable store.
//!
//! Where [`RedbBackend`](crate::RedbBackend) is an opaque embedded database, a
//! zip archive is a store anyone can open: each key is an entry name and each
//! value is that entry's bytes, so a consumer that names its keys after real
//! files (`manifest.cbor`, `media/<hash>.wav`) produces an archive a person can
//! unzip and read without the app that wrote it. That is its reason to exist:
//! interoperable, no-lock-in project files.
//!
//! It is **snapshot-oriented**, not a log store. The whole archive is held in
//! memory and every mutating call rewrites the file atomically (temp file, then
//! rename). That suits a consumer that saves an entire bundle at once through
//! [`apply`](Backend::apply); it is the wrong backend for high-frequency
//! incremental appends, which should use redb. Reads (`get`, `list`, `scan`)
//! serve from the in-memory map, so they are cheap. Entries are Deflate-
//! compressed (miniz_oxide), so structured and silence-heavy values shrink while
//! staying readable by any zip tool.
//!
//! Native only, behind the `zip` feature: it uses filesystem paths. The browser
//! reaches for an OPFS-backed store instead, the same split as redb.
//!
//! Single-writer: there is no cross-process file lock. One process serializes
//! its own writes through the internal mutex, but two processes that opened the
//! same archive each rewrite the whole file, so the later writer's snapshot wins
//! and the earlier one's changes are lost. That fits a project-file model where
//! one app instance owns a file at a time; a consumer needing concurrent writers
//! wants a transactional backend (redb), not this one.

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
// Leading `::` so this resolves to the external crate, not this module.
use ::zip::write::SimpleFileOptions;
use ::zip::{CompressionMethod, ZipArchive, ZipWriter};

use crate::backend::{Backend, WriteOp};
use crate::error::StoreError;

/// Wrap any zip or I/O error as a backend failure, keeping the seam agnostic.
fn backend(err: impl std::fmt::Display) -> StoreError {
    StoreError::Backend(err.to_string())
}

/// Distinguishes concurrent temp files without a clock: one process serializes
/// its saves, but two processes writing sibling archives must not collide.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct Inner {
    path: PathBuf,
    entries: BTreeMap<String, Vec<u8>>,
}

/// A [`Backend`] over a single zip archive: muniment's portable, inspectable
/// desktop store. Cheap to clone (the archive is shared behind an `Arc`).
#[derive(Clone)]
pub struct ZipBackend {
    inner: Arc<Mutex<Inner>>,
}

impl ZipBackend {
    /// Open the archive at `path`, or start an empty one if it does not exist.
    ///
    /// An existing archive is read fully into memory up front, so later reads do
    /// not touch the disk and a mutating call can rewrite the whole file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let entries = if path.exists() {
            read_entries(&path)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner { path, entries })),
        })
    }
}

fn read_entries(path: &Path) -> Result<BTreeMap<String, Vec<u8>>, StoreError> {
    let file = std::fs::File::open(path).map_err(backend)?;
    let mut archive = ZipArchive::new(file).map_err(backend)?;
    let mut entries = BTreeMap::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(backend)?;
        // Zip marks a "directory" by a trailing '/' with no content. Skip only
        // those empty markers: an opaque key that happens to end in '/' carries
        // bytes and must round-trip. (A genuinely empty-valued key ending in '/'
        // is indistinguishable from a directory marker in the zip format and is
        // the one key shape not guaranteed to survive a reopen.)
        if entry.is_dir() && entry.size() == 0 {
            continue;
        }
        let name = entry.name().to_string();
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut bytes).map_err(backend)?;
        entries.insert(name, bytes);
    }
    Ok(entries)
}

/// Serialize `entries` to a zip archive and atomically, durably replace `path`.
///
/// Entries write in `BTreeMap` order, so an unchanged map produces a byte-stable
/// archive. Compression is chosen per entry (see [`worth_deflating`]): structured
/// or repetitive data is `Deflated` (miniz_oxide, deterministic and universally
/// readable), while already-compressed payloads (e.g. WavPack media) are `Stored`
/// to skip the wasted compression pass. Both methods are readable by any zip tool.
///
/// Durability: the temp file is `fsync`ed before the rename, so a crash or power
/// loss cannot leave the target name pointing at unwritten blocks, and the parent
/// directory is `fsync`ed after (unix) so the rename itself survives. A failure
/// at any step removes the temp file rather than leaving it beside the archive.
fn write_entries(path: &Path, entries: &BTreeMap<String, Vec<u8>>) -> Result<(), StoreError> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in entries {
        let method = if worth_deflating(bytes) {
            CompressionMethod::Deflated
        } else {
            CompressionMethod::Stored
        };
        let options = SimpleFileOptions::default().compression_method(method);
        writer.start_file(name.as_str(), options).map_err(backend)?;
        writer.write_all(bytes).map_err(backend)?;
    }
    let buffer = writer.finish().map_err(backend)?.into_inner();

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = parent.join(format!(".muniment-zip-{}-{}.tmp", std::process::id(), counter));

    if let Err(error) = write_synced(&tmp, &buffer) {
        let _ = std::fs::remove_file(&tmp);
        return Err(error);
    }
    if let Err(error) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(backend(error));
    }
    sync_parent_dir(path);
    Ok(())
}

/// Cheap estimate of whether an entry is worth Deflate-compressing, so we don't
/// spend a full compression pass on data that is already compressed (WavPack
/// media, images, etc.). Uses the Shannon entropy of a prefix as a proxy: near
/// 8 bits/byte means high-entropy (incompressible) data, store it as-is; lower
/// means structure Deflate can exploit. This only affects size/CPU, never
/// correctness (both methods round-trip), so an occasional wrong guess is fine.
fn worth_deflating(data: &[u8]) -> bool {
    // Tiny entries: the header overhead dominates either way; just deflate.
    if data.len() < 256 {
        return true;
    }
    let sample = &data[..data.len().min(16384)];
    let mut hist = [0u32; 256];
    for &b in sample {
        hist[b as usize] += 1;
    }
    let n = sample.len() as f64;
    let entropy: f64 = hist
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| {
            let p = c as f64 / n;
            -p * p.log2()
        })
        .sum();
    // Below ~7.5 bits/byte there is usually enough redundancy for Deflate to win.
    entropy < 7.5
}

/// Write bytes to `tmp` and flush them to stable storage before it is used.
fn write_synced(tmp: &Path, buffer: &[u8]) -> Result<(), StoreError> {
    let mut file = std::fs::File::create(tmp).map_err(backend)?;
    file.write_all(buffer).map_err(backend)?;
    file.sync_all().map_err(backend)?;
    Ok(())
}

/// Flush the directory entry created by the rename. Unix-only; a best-effort
/// durability step, so failures here are not surfaced.
#[cfg(unix)]
fn sync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) {}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Backend for ZipBackend {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.inner.lock().unwrap().entries.get(key).cloned())
    }

    /// Atomic: the in-memory map is rolled back if the archive rewrite fails, so
    /// a failed `put` leaves both disk and memory as they were.
    async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        let previous = inner.entries.insert(key.to_string(), bytes.to_vec());
        if let Err(error) = write_entries(&inner.path, &inner.entries) {
            match previous {
                Some(old) => inner.entries.insert(key.to_string(), old),
                None => inner.entries.remove(key),
            };
            return Err(error);
        }
        Ok(())
    }

    /// Atomic like `put`. Deleting an absent key is a no-op that touches neither
    /// disk nor memory, so it never rewrites the archive.
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        let Some(previous) = inner.entries.remove(key) else {
            return Ok(());
        };
        if let Err(error) = write_entries(&inner.path, &inner.entries) {
            inner.entries.insert(key.to_string(), previous);
            return Err(error);
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .entries
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect())
    }

    async fn scan(&self, start: &str, end: &str) -> Result<Vec<String>, StoreError> {
        // A degenerate or inverted range is empty, matching MemoryBackend.
        // Guarding here also avoids `BTreeMap::range` panicking on `start > end`,
        // which would poison the shared mutex.
        if start >= end {
            return Ok(Vec::new());
        }
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .entries
            .range(start.to_string()..end.to_string())
            .map(|(key, _)| key.clone())
            .collect())
    }

    /// Atomic per the seam contract: the batch is applied to a working copy,
    /// the archive is rewritten, and only a successful rewrite commits the copy
    /// to memory. A failed write leaves both the file and the map unchanged.
    async fn apply(&self, ops: &[WriteOp]) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().unwrap();
        let mut next = inner.entries.clone();
        for op in ops {
            match op {
                WriteOp::Put { key, value } => {
                    next.insert(key.clone(), value.clone());
                }
                WriteOp::Delete { key } => {
                    next.remove(key);
                }
            }
        }
        write_entries(&inner.path, &next)?;
        inner.entries = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_backend() -> (tempfile::TempDir, ZipBackend) {
        let dir = tempfile::tempdir().unwrap();
        let backend = ZipBackend::open(dir.path().join("store.zip")).unwrap();
        (dir, backend)
    }

    #[test]
    fn put_get_delete_round_trip() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            assert_eq!(b.get("k").await.unwrap(), None);
            b.put("k", b"v").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), Some(b"v".to_vec()));
            b.put("k", b"v2").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), Some(b"v2".to_vec()));
            b.delete("k").await.unwrap();
            assert_eq!(b.get("k").await.unwrap(), None);
            b.delete("k").await.unwrap();
        });
    }

    #[test]
    fn list_returns_only_the_prefix() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            for k in ["media/a", "media/b", "manifest.cbor"] {
                b.put(k, b"v").await.unwrap();
            }
            let mut media = b.list("media/").await.unwrap();
            media.sort();
            assert_eq!(media, vec!["media/a".to_string(), "media/b".to_string()]);
            assert_eq!(
                b.list("manifest").await.unwrap(),
                vec!["manifest.cbor".to_string()]
            );
            assert!(b.list("none/").await.unwrap().is_empty());
        });
    }

    #[test]
    fn scan_returns_the_range_in_ascending_order() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
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
                    key: "manifest.cbor".into(),
                    value: b"header".to_vec(),
                },
                WriteOp::Put {
                    key: "media/x.wav".into(),
                    value: b"body".to_vec(),
                },
                WriteOp::Delete { key: "drop".into() },
            ])
            .await
            .unwrap();
            assert_eq!(b.get("manifest.cbor").await.unwrap(), Some(b"header".to_vec()));
            assert_eq!(b.get("media/x.wav").await.unwrap(), Some(b"body".to_vec()));
            assert_eq!(b.get("drop").await.unwrap(), None);
            assert_eq!(b.get("keep").await.unwrap(), Some(b"v".to_vec()));
        });
    }

    #[test]
    fn scan_with_an_inverted_range_is_empty_not_a_panic() {
        pollster::block_on(async {
            let (_dir, b) = temp_backend();
            for k in ["a", "m", "z"] {
                b.put(k, b"v").await.unwrap();
            }
            // Inverted range: BTreeMap::range would panic (poisoning the shared
            // mutex); the backend must return empty like MemoryBackend does.
            assert!(b.scan("z", "a").await.unwrap().is_empty());
            // The backend is still usable afterward — the mutex is not poisoned.
            assert_eq!(b.get("m").await.unwrap(), Some(b"v".to_vec()));
        });
    }

    #[test]
    fn a_content_key_ending_in_slash_round_trips() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.zip");
            {
                let b = ZipBackend::open(&path).unwrap();
                b.put("campaign/", b"data").await.unwrap();
                b.put("media/a.wav", b"audio").await.unwrap();
            }
            // A key ending in '/' but carrying bytes must survive a reopen; only
            // empty directory markers are skipped.
            let reopened = ZipBackend::open(&path).unwrap();
            assert_eq!(reopened.get("campaign/").await.unwrap(), Some(b"data".to_vec()));
            assert_eq!(reopened.get("media/a.wav").await.unwrap(), Some(b"audio".to_vec()));
        });
    }

    #[test]
    fn incompressible_entries_are_stored_compressible_ones_deflated() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("mixed.zip");

            // High-entropy bytes (a simple LCG), incompressible like WavPack media.
            let mut noise = vec![0u8; 40_000];
            let mut x: u32 = 0x1234_5678;
            for b in noise.iter_mut() {
                x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                *b = (x >> 24) as u8;
            }
            // Structured, repetitive bytes that Deflate crushes.
            let structured = vec![b'a'; 40_000];

            {
                let store = ZipBackend::open(&path).unwrap();
                store.put("media/x.wv", &noise).await.unwrap();
                store.put("manifest.cbor", &structured).await.unwrap();
            }

            // Both round-trip regardless of method.
            let store = ZipBackend::open(&path).unwrap();
            assert_eq!(store.get("media/x.wv").await.unwrap().unwrap(), noise);
            assert_eq!(store.get("manifest.cbor").await.unwrap().unwrap(), structured);

            // Inspect the real zip: the noise entry is Stored, the structured one
            // Deflated and much smaller.
            let file = std::fs::File::open(&path).unwrap();
            let mut archive = ZipArchive::new(file).unwrap();
            let noise_entry = archive.by_name("media/x.wv").unwrap();
            assert_eq!(noise_entry.compression(), ::zip::CompressionMethod::Stored);
            assert!(noise_entry.compressed_size() >= noise.len() as u64);
            drop(noise_entry);
            let struct_entry = archive.by_name("manifest.cbor").unwrap();
            assert_eq!(struct_entry.compression(), ::zip::CompressionMethod::Deflated);
            assert!(struct_entry.compressed_size() < structured.len() as u64 / 10);
        });
    }

    #[test]
    fn state_survives_reopen_as_a_real_zip() {
        pollster::block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("store.zip");
            {
                let b = ZipBackend::open(&path).unwrap();
                b.put("manifest.cbor", b"durable").await.unwrap();
            }
            // A standard zip reader — not ZipBackend — can open the file.
            let file = std::fs::File::open(&path).unwrap();
            let mut archive = ZipArchive::new(file).unwrap();
            let mut entry = archive.by_name("manifest.cbor").unwrap();
            let mut bytes = Vec::new();
            entry.read_to_end(&mut bytes).unwrap();
            assert_eq!(bytes, b"durable");

            // And a fresh ZipBackend handle sees the same state.
            drop(entry);
            drop(archive);
            let reopened = ZipBackend::open(&path).unwrap();
            assert_eq!(
                reopened.get("manifest.cbor").await.unwrap(),
                Some(b"durable".to_vec())
            );
        });
    }
}
