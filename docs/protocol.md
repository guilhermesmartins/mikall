# mikall wire protocol (codename `negi`)

Status: M0 defines the model; M1 puts it on libp2p. Encoding is CBOR
(ciborium); signatures are computed over the exact serialized bytes carried
on the wire — sign the bytes, never re-serialize-then-verify.

## Envelope

```
Envelope {
  v: u8,                      // protocol version = 1
  author: [u8; 32],           // ed25519 public key
  kind: Kind,                 // ChanMsg | DmMsg | Topic | Membership |
                              // PresenceBeacon | CallSignal | FileOffer
  channel: Option<[u8; 32]>,  // ChannelId for channel kinds
  lamport: u64,
  parents: [MessageId],       // BLAKE3 ids — DAG links (ChanMsg/DmMsg)
  ts_hint: u64,               // unix ms, advisory only, never used to order
  payload: bytes,             // plaintext (public channel) or ciphertext
  sig: [u8; 64],              // ed25519 over all preceding fields
}
MessageId = BLAKE3(envelope minus sig)
```

## GossipSub topics

- `/mikall/1/chan/<channel-id>` — messages, topic changes, membership ops
- `/mikall/1/chan/<channel-id>/presence` — beacons every 60 s, expire at 150 s
- `/mikall/1/call/<call-id>/signal` — group-call signaling fanout

Public `ChannelId = BLAKE3("mikall:chan:" + canonical_name)` so `JOIN #stage`
converges on one topic everywhere. Private channels use a random 32-byte id
shared via invite. Gossipsub message-id = our `MessageId` (dedup); transport
signing off (identity keys sign at the envelope layer); mesh n = 6.

## Ordering and history sync

No CRDT library: an append-only chat log needs only causal order. Each
message carries a Lamport counter and `parents` = the DAG heads its author
saw. Display order = topological sort tie-broken by
`(lamport, author, message_id)` — deterministic for any arrival order (a
property pinned by proptests and BDD scenarios).

Anti-entropy (`/mikall/sync/1`, request-response): exchange heads → peer
lists missing ids → batch fetch (≤256/request). Unknown parents raise
`HistoryGapDetected` and trigger backfill. Deletion is local-only plus an
advisory tombstone for one's own messages — remote deletion cannot be forced
on a serverless network and mikall does not pretend otherwise.

## DHT records (Kademlia)

- Channel discovery: provider records under
  `BLAKE3("mikall:chan-prov:" + channel_id)` plus a signed
  `ChannelAnnounce { name, channel_id, founder_sig }` for name lookup —
  first founder wins a name (squatting is acknowledged in security.md).
- Identity: signed `IdentityRecord { peer_id, addrs, x25519_prekeys, sig }`
  under `BLAKE3("mikall:id:" + pubkey)`, republished every 12 h, TTL 24 h.
- Relays: AutoNAT-confirmed public nodes provide under `mikall:relay:v1`;
  NATed peers reserve circuit slots, then upgrade via DCUtR hole punching.

## End-to-end encryption (staged)

- v1: `crypto_box` sealed box per DM (X25519 + XChaCha20-Poly1305). Simple
  and correct, **no forward secrecy** — stated, not hidden.
- v2: `vodozemac` — Olm double-ratchet for DMs, Megolm outbound group
  sessions for private channels with epoch rotation on every membership
  change (the epoch already lives in `ChannelVisibility::Private`).
- Public channels: signed plaintext (they are public).

## File transfer (`/mikall/blob/1`)

256 KiB chunks; BLAKE3 per chunk plus a root hash in the signed manifest.
Receiver requests chunk ranges (8 in flight), verifies each chunk against
the manifest before it counts (`ChunkBitmap`), resumes from the bitmap after
disconnects. Any peer holding the blob may serve it.

## Calls (`/mikall/media/1`)

Signaling travels as sealed DM envelopes (`Offer/Accept/Decline` carrying a
per-call media key), then a dedicated stream (QUIC where possible,
DCUtR-upgraded when relayed — relayed calls are flagged in the UI). Media
framing: 12-byte header `{ seq: u16, ts: u32 (48 kHz), ssrc: u32, kind: u8,
flags: u8 }` + Opus (voice) or H.264 (screen) payload, each frame
AEAD-encrypted (nonce = ssrc ‖ seq ‖ counter). Group calls are a full mesh
capped at 8 participants — an SFU would be a server. Jitter buffer adaptive
20–200 ms; Opus in-band FEC plus decoder PLC.
