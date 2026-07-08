# muniment Founding Proposal

**Date:** 2026-07-08
**Status:** founding proposal. This repo's first doc. Unlike vates, sibylla, and
armillary (promoted from mere), muniment is a **fresh build** designed from a
survey of four consumers that were each hand-rolling the same seam. The store,
the two access patterns, and the codec are ported to code and green in this
commit.

## 1. What muniment is

muniment is a portable persistence store: a small contract for durable bytes,
plus the two access patterns every consumer needs over it.

- **The backend seam.** `Backend` is a host-supplied key/value byte store,
  `async` and `?Send`. The host realizes it as the filesystem on desktop, OPFS
  in the browser, or an embedded store (redb, fjall). muniment defines the
  contract and ships only `MemoryBackend`, the in-memory test floor. `?Send`
  because a browser main thread can only await OPFS promises (non-`Send`
  futures); desktop backends do synchronous I/O and return ready futures, so the
  bound costs them nothing.
- **Mutable slots.** `SlotStore` holds typed named cells: `save` overwrites,
  `load` returns the latest. This is the current-session, current-campaign,
  current-project pattern. It serializes through a pluggable `Codec` (JSON and
  postcard ship; a consumer can supply CBOR or rkyv), so muniment mandates no
  wire format.
- **Content-addressed blobs.** `BlobStore` holds immutable chunks: `put` returns
  a blake3 `Hash`, `get` fetches by it. Identical content is stored once; a new
  version is new bytes with a new hash, never a mutation.

muniment moves bytes. It does not model what they mean.

## 2. Why a standalone crate

The survey (2026-07-08, recorded in the workspace memory) read the persistence
surface of four apps:

- **woodshed** already had the target shape: a core `Storage` trait realized as
  `std::fs` on desktop and OPFS in the browser, in a serval-host crate.
- **strophe** hand-rolls it: a `ProjectBundle` with `to_bytes`/`from_bytes`, the
  host does the I/O, media travels content-addressed by `MediaRef`.
- **isometry** hand-rolls it: pure serde documents (maps, campaigns, event
  logs), the host does the I/O.
- **mere/eidetic** has the heavyweight version: an async `Store` with
  Request/Response, fjall/redb/OPFS/iroh backends, and content-hashed engrams.

Three things recurred in all four: a host-realized backend seam, two access
patterns (mutable slot and content-addressed blob), and format-agnostic byte
movement. Every consumer was rebuilding the seam. muniment is that seam, factored
out once, the same one-way pattern as the wgpu-sibling libs. It depends on no
app; apps depend on it.

## 3. Design decisions

- **Async `?Send` seam, not sync.** woodshed's trait is sync, which works on
  desktop and inside an OPFS worker but not on a browser main thread. The async
  `?Send` seam is the general case that works in every target, and desktop
  backends return ready futures. A sync convenience wrapper for desktop-only
  hosts is a roadmap item (section 4), not a second canonical trait.
- **Format-agnostic via a codec type parameter.** `SlotStore<B, C>` picks the
  codec at the type level; `JsonSlots<B>` and `PostcardSlots<B>` are the aliases.
  This directly serves the survey: woodshed wants JSON, strophe and isometry want
  postcard.
- **Two access patterns as separate types.** `SlotStore` and `BlobStore` are
  distinct, so a consumer that only wants mutable slots (woodshed settings)
  ignores content-addressing, and one that wants immutable chunks (strophe media)
  ignores slots.
- **Cheap-to-clone backends.** `MemoryBackend` is an `Arc`-backed handle, so one
  backend seeds both a slot store and a blob store, mirroring how a real fs or
  OPFS handle clones cheaply.

## 4. Roadmap

Done-conditions, not time estimates.

- **P0 (this commit): the seam and the two stores.** `Backend`, `SlotStore`,
  `BlobStore`, `Codec`, `MemoryBackend`. **Done when** `cargo test` is green (it
  is, 10 tests on the JSON default and with postcard).
- **P1: real backends.** An `FsBackend` (desktop, `directories` + `std::fs`) and
  an `OpfsBackend` (browser), shipped as feature-gated modules or sibling crates.
  **Done when** woodshed's `FsStorage` is expressible as an `FsBackend` and a
  browser host persists through OPFS on the same `SlotStore` API.
- **P2: a sync convenience.** A `SyncBackend` trait plus an adapter that presents
  a sync backend as an async `Backend` (ready futures), for desktop/worker-only
  hosts that would rather not thread async through their code.
- **P3: consumer adoption.** woodshed, then strophe and isometry, move their
  persistence onto muniment. **Done when** at least one app ships its durable
  state through muniment's slot and blob stores.

## 5. Scope: what muniment is not

- **Not the log.** The append-only, replayable log that *versions* what muniment
  stores is its sibling crate, codicil. muniment holds the current bytes; codicil
  holds the sequence of edits.
- **Not the content model.** The note-and-tag-and-list layer that users author is
  above both muniment and codicil. That layer is trending toward a
  content-addressed container graph (nodes as addressable containers whose
  content lives in muniment blobs, spanning local files, media, and web
  documents across schemes), which is exactly why `BlobStore` is a first-class
  half of this crate and not an afterthought. But that model is a separate
  design; muniment only guarantees the storage under it.
- **Not a database.** No queries, no indices, no transactions. A backend may be a
  database; muniment's contract is get/put/delete/list.

## 6. Licensing

Fresh code, licensed MPL-2.0 to match the sibling promoted crates (vates,
sibylla, armillary). Whether the family relicenses to `MIT OR Apache-2.0` before
first publish is Mark's call, decided together so the crates match. Until then
MPL is the shared default.

## Provenance

Grounded in a read of woodshed (`woodshed-core/storage.rs`,
`woodshed-serval/storage.rs`), strophe (`strophe-model/persistence.rs`), isometry
(`isometry-core`, CLAUDE.md), and mere's eidetic (`eidetic-core`) on 2026-07-08.
The survey and the two-crate decision (muniment for the store, codicil for the
log) are recorded in the workspace memory.
