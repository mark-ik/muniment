# muniment

A portable persistence store. One host-supplied byte `Backend` seam (filesystem,
OPFS, redb, fjall), typed mutable `SlotStore` slots over a pluggable codec, and
content-addressed immutable `BlobStore` blobs. Format-agnostic and
wasm-friendly. The storage floor a small app keeps its durable state in.

```rust
use muniment::{BlobStore, JsonSlots, MemoryBackend};

# pollster::block_on(async {
let backend = MemoryBackend::new();               // host swaps in fs / OPFS
let slots = JsonSlots::new(backend.clone());
let blobs = BlobStore::new(backend);

slots.save("session", &("Practice", 96.0)).await?; // mutable named slot
let session: Option<(String, f32)> = slots.load("session").await?;

let hash = blobs.put(b"media bytes").await?;        // content-addressed blob
assert_eq!(blobs.get(&hash).await?.unwrap(), b"media bytes");
# Ok::<_, muniment::StoreError>(()) }).unwrap();
```

The `Backend` is `async` and `?Send` so a browser main thread can await OPFS
promises, while desktop backends return ready futures and pay nothing. muniment
defines the seam and ships only an in-memory backend; the host supplies the real
one. It moves bytes and does not model what they mean.

Built from a survey of four consumers (woodshed, hocket, isometry, mere), each
of which was hand-rolling this seam. Sibling to
[codicil](https://github.com/mark-ik/codicil), the append-only log that versions
what muniment stores. See [`design_docs/`](design_docs/).

The name: a muniment room is where a household keeps its deeds and records, the
documents preserved as evidence.

License: dual MIT OR Apache-2.0, at your option.
