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
- Relays: discovered from *connected* peers, not the DHT — every mikall
  node advertising the circuit-relay hop protocol via identify is a
  candidate (a `mikall:relay:v1` provider record for finding relays
  beyond the connected set remains future work). See "NAT traversal"
  below.

## NAT traversal (how NATed peers connect)

No servers: every traversal capability is a peer role, composed into
every node's swarm (M18, stock rust-libp2p behaviours).

- **Reachability** — autonat v1. Every node is probe client *and* probe
  server: connected peers dial each other back to establish a
  Public/Private verdict. Identify supplies the observed-address
  candidates the probes confirm.
- **Peer-run relays** — circuit-relay v2. Every node offers relay
  service by default (`NetConfig::relay_service`), capped conservatively
  (`RelayLimits`): 8 reservations, 8 circuits (2 per source peer),
  10 minutes and 8 MiB per direction per circuit. The caps are the honesty: a circuit
  exists to carry signaling and to bridge until hole punching lands —
  sustained media over someone else's uplink hits the byte cap in
  minutes *by design*.
- **Reserving** — a node whose verdict is Private automatically obtains
  a reservation from a connected relay-capable peer and from then on
  publishes a `/…/p2p/RELAY/p2p-circuit/p2p/SELF` address among its
  listen addresses — the same list the settings → network pane and
  `mikalld /addr` already show, so a NATed node hands out a
  dialable-from-anywhere address with zero extra UI. Lost reservations
  are re-acquired automatically; a Public verdict drops them.
- **Upgrading** — DCUtR. Once a relayed connection exists, both ends
  hole punch toward a direct connection and traffic migrates off the
  relay; if the punch fails (symmetric NATs do exist), the relayed
  connection persists within the caps above, so calls and media over a
  never-upgraded path degrade honestly rather than silently.

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
