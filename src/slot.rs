/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Named mutable slots: typed save and load of a document at a key.
//!
//! A slot is a mutable named cell. `save` overwrites what was there; `load`
//! returns the current value or `None`. This is the woodshed-session,
//! isometry-campaign, strophe-project pattern: one document you keep the latest
//! of. For immutable, content-addressed chunks (media, log entries), use
//! [`BlobStore`](crate::BlobStore) instead.

use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Serialize};

use crate::backend::Backend;
use crate::codec::Codec;
use crate::error::StoreError;

/// Typed mutable slots over a [`Backend`], serialized through a [`Codec`].
///
/// `SlotStore<B, C>` picks the codec at the type level. For the JSON default use
/// the [`JsonSlots`](crate::JsonSlots) alias; for postcard, `SlotStore<B,
/// PostcardCodec>`.
pub struct SlotStore<B, C> {
    backend: B,
    _codec: PhantomData<fn() -> C>,
}

impl<B: Backend, C: Codec> SlotStore<B, C> {
    /// Wrap a backend.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            _codec: PhantomData,
        }
    }

    /// The backend this store writes through.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Serialize `value` and write it at `key`, overwriting any previous value.
    pub async fn save<T: Serialize>(&self, key: &str, value: &T) -> Result<(), StoreError> {
        let bytes = C::encode(value)?;
        self.backend.put(key, &bytes).await
    }

    /// Load and deserialize the value at `key`, or `None` if the slot is empty.
    pub async fn load<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StoreError> {
        match self.backend.get(key).await? {
            Some(bytes) => Ok(Some(C::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Clear the slot at `key`. Absent keys are not an error.
    pub async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.backend.delete(key).await
    }

    /// Every slot key beginning with `prefix`.
    pub async fn keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.backend.list(prefix).await
    }
}

#[cfg(all(test, feature = "json"))]
mod tests {
    use super::*;
    use crate::backend::MemoryBackend;
    use crate::codec::JsonCodec;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Session {
        tab: String,
        bpm: f32,
    }

    fn store() -> SlotStore<MemoryBackend, JsonCodec> {
        SlotStore::new(MemoryBackend::new())
    }

    #[test]
    fn save_then_load_round_trips() {
        pollster::block_on(async {
            let s = store();
            let session = Session {
                tab: "Practice".into(),
                bpm: 96.0,
            };
            s.save("session", &session).await.unwrap();
            let back: Option<Session> = s.load("session").await.unwrap();
            assert_eq!(back, Some(session));
        });
    }

    #[test]
    fn missing_slot_is_none() {
        pollster::block_on(async {
            let s = store();
            let back: Option<Session> = s.load("absent").await.unwrap();
            assert_eq!(back, None);
        });
    }

    #[test]
    fn save_overwrites() {
        pollster::block_on(async {
            let s = store();
            s.save("k", &1u32).await.unwrap();
            s.save("k", &2u32).await.unwrap();
            assert_eq!(s.load::<u32>("k").await.unwrap(), Some(2));
        });
    }

    #[test]
    fn delete_clears() {
        pollster::block_on(async {
            let s = store();
            s.save("k", &1u32).await.unwrap();
            s.delete("k").await.unwrap();
            assert_eq!(s.load::<u32>("k").await.unwrap(), None);
        });
    }

    #[test]
    fn keys_lists_by_prefix() {
        pollster::block_on(async {
            let s = store();
            s.save("note/a", &1u32).await.unwrap();
            s.save("note/b", &2u32).await.unwrap();
            s.save("other", &3u32).await.unwrap();
            let mut keys = s.keys("note/").await.unwrap();
            keys.sort();
            assert_eq!(keys, vec!["note/a".to_string(), "note/b".to_string()]);
        });
    }
}
