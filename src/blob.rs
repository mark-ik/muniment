/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Content-addressed immutable blobs.
//!
//! A blob is stored under the blake3 hash of its bytes, so identical content is
//! stored once and every reference is by hash. This is the strophe-media,
//! eidetic-engram pattern: immutable chunks that never change in place. A new
//! version is new bytes with a new hash, never a mutation of the old.

use serde::{Deserialize, Serialize};

use crate::backend::Backend;
use crate::error::StoreError;

const BLOB_PREFIX: &str = "blob/";

/// A blake3 content hash. Stable: the same bytes always hash the same, on every
/// platform.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Hash `bytes`.
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// The 32 raw hash bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The lowercase hex form (64 chars).
    pub fn to_hex(&self) -> String {
        blake3::Hash::from_bytes(self.0).to_hex().to_string()
    }

    /// Parse a hex form back into a hash, or `None` if it is not 64 hex chars.
    pub fn from_hex(s: &str) -> Option<Self> {
        blake3::Hash::from_hex(s).ok().map(|h| Self(*h.as_bytes()))
    }
}

impl std::fmt::Debug for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", self.to_hex())
    }
}

impl std::fmt::Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_hex())
    }
}

/// Content-addressed immutable blob storage over a [`Backend`].
pub struct BlobStore<B> {
    backend: B,
}

impl<B: Backend> BlobStore<B> {
    /// Wrap a backend.
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    /// The backend this store writes through.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Store `bytes`, returning their content hash. Idempotent: the same bytes
    /// yield the same hash and an identical write.
    pub async fn put(&self, bytes: &[u8]) -> Result<Hash, StoreError> {
        let hash = Hash::of(bytes);
        self.backend.put(&key(&hash), bytes).await?;
        Ok(hash)
    }

    /// Fetch the bytes for `hash`, or `None` if absent.
    pub async fn get(&self, hash: &Hash) -> Result<Option<Vec<u8>>, StoreError> {
        self.backend.get(&key(hash)).await
    }

    /// Whether a blob for `hash` is present.
    pub async fn has(&self, hash: &Hash) -> Result<bool, StoreError> {
        Ok(self.backend.get(&key(hash)).await?.is_some())
    }
}

fn key(hash: &Hash) -> String {
    format!("{BLOB_PREFIX}{}", hash.to_hex())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;

    #[test]
    fn hash_is_content_stable() {
        assert_eq!(Hash::of(b"hello"), Hash::of(b"hello"));
        assert_ne!(Hash::of(b"hello"), Hash::of(b"world"));
    }

    #[test]
    fn hex_round_trips() {
        let h = Hash::of(b"the quick brown fox");
        assert_eq!(Hash::from_hex(&h.to_hex()), Some(h));
        assert_eq!(h.to_hex().len(), 64);
    }

    #[test]
    fn put_returns_content_hash_and_get_round_trips() {
        pollster::block_on(async {
            let store = BlobStore::new(MemoryBackend::new());
            let h = store.put(b"media bytes").await.unwrap();
            assert_eq!(h, Hash::of(b"media bytes"));
            assert_eq!(store.get(&h).await.unwrap(), Some(b"media bytes".to_vec()));
            assert!(store.has(&h).await.unwrap());
        });
    }

    #[test]
    fn identical_content_dedupes_to_one_key() {
        pollster::block_on(async {
            let backend = MemoryBackend::new();
            let store = BlobStore::new(backend.clone());
            let a = store.put(b"same").await.unwrap();
            let b = store.put(b"same").await.unwrap();
            assert_eq!(a, b);
            assert_eq!(backend.len(), 1, "identical content stored once");
        });
    }

    #[test]
    fn missing_blob_is_none() {
        pollster::block_on(async {
            let store = BlobStore::new(MemoryBackend::new());
            let h = Hash::of(b"never stored");
            assert_eq!(store.get(&h).await.unwrap(), None);
            assert!(!store.has(&h).await.unwrap());
        });
    }
}
