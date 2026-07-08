/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

//! Pluggable serialization codec for the typed [`SlotStore`](crate::SlotStore).
//!
//! muniment moves bytes; the codec decides how a typed value becomes bytes. The
//! store is generic over `C: Codec`, so a consumer picks the format its data
//! wants: JSON for human-readable notes, postcard for deterministic compact
//! state. Two codecs ship behind features; a consumer can supply its own (CBOR,
//! rkyv) by implementing this trait.

use serde::{de::DeserializeOwned, Serialize};

use crate::error::StoreError;

/// Converts typed values to and from bytes. A codec is a marker type; the
/// methods are associated functions, so a `SlotStore<B, C>` needs no codec
/// instance.
pub trait Codec {
    /// Serialize `value` to bytes.
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError>;

    /// Deserialize bytes back into a `T`.
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError>;
}

/// A JSON codec (serde_json). Human-readable; the friendly default for notes and
/// settings. Enabled by the `json` feature (on by default).
#[cfg(feature = "json")]
pub struct JsonCodec;

#[cfg(feature = "json")]
impl Codec for JsonCodec {
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
        serde_json::to_vec(value).map_err(|e| StoreError::Codec(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
        serde_json::from_slice(bytes).map_err(|e| StoreError::Codec(e.to_string()))
    }
}

/// A postcard codec: compact binary, deterministic given equal input (a
/// prerequisite for content-addressing serialized state). Enabled by the
/// `postcard` feature.
#[cfg(feature = "postcard")]
pub struct PostcardCodec;

#[cfg(feature = "postcard")]
impl Codec for PostcardCodec {
    fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, StoreError> {
        postcard::to_allocvec(value).map_err(|e| StoreError::Codec(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, StoreError> {
        postcard::from_bytes(bytes).map_err(|e| StoreError::Codec(e.to_string()))
    }
}
