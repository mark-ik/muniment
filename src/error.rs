//! The store error type.

use std::fmt;

/// A storage failure.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    /// The host backend (filesystem, OPFS, redb, fjall, ...) failed. The string
    /// is the backend's own error, stringified so the seam stays backend-agnostic.
    Backend(String),
    /// Encoding or decoding a typed value through a [`Codec`](crate::Codec) failed.
    Codec(String),
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Backend(msg) => write!(f, "backend: {msg}"),
            StoreError::Codec(msg) => write!(f, "codec: {msg}"),
        }
    }
}

impl std::error::Error for StoreError {}
