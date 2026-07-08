//! muniment — a portable persistence store.
//!
//! A muniment room is where a household keeps its records: the deeds and
//! documents preserved as evidence. This crate is that room for an app's durable
//! state, and nothing more.
//!
//! Three pieces over one seam:
//!
//! - [`Backend`] is the host-supplied byte store. The host realizes it as the
//!   filesystem on desktop, OPFS in the browser, or an embedded store (redb,
//!   fjall). It is `async` and `?Send` so a browser main thread can await OPFS
//!   promises; desktop backends return ready futures and pay nothing. muniment
//!   ships only [`MemoryBackend`], the in-memory test floor.
//! - [`SlotStore`] holds typed **mutable named slots**: `save` overwrites, `load`
//!   returns the latest. The current-session, current-campaign, current-project
//!   pattern. Serialized through a pluggable [`Codec`] (JSON or postcard here,
//!   your own otherwise), so muniment mandates no wire format.
//! - [`BlobStore`] holds **content-addressed immutable blobs**: `put` returns a
//!   blake3 [`Hash`], `get` fetches by it. Identical content is stored once; a
//!   new version is new bytes with a new hash, never a mutation.
//!
//! muniment holds bytes; it does not model what they mean. The append-only log
//! that versions them is its sibling, codicil; the note-and-tag content model is
//! a layer above both.

pub mod backend;
pub mod blob;
pub mod codec;
pub mod error;
pub mod slot;

pub use backend::{Backend, MemoryBackend};
pub use blob::{BlobStore, Hash};
pub use codec::Codec;
pub use error::StoreError;
pub use slot::SlotStore;

#[cfg(feature = "json")]
pub use codec::JsonCodec;
#[cfg(feature = "postcard")]
pub use codec::PostcardCodec;

/// [`SlotStore`] with the JSON codec, the friendly default.
#[cfg(feature = "json")]
pub type JsonSlots<B> = slot::SlotStore<B, codec::JsonCodec>;

/// [`SlotStore`] with the deterministic postcard codec.
#[cfg(feature = "postcard")]
pub type PostcardSlots<B> = slot::SlotStore<B, codec::PostcardCodec>;
