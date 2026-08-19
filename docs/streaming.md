# Streaming: how screen-share video moves

This document adapts the screen-sharing architecture consult into the
project's plan of record. The problem it solves: p2p full-mesh media makes
the *sharer* pay for every viewer — N−1 uplinks and encodes on exactly the
machine least able to afford them (weak CPU, slow uplink, 10+ viewers).
The goal of the whole design is one sentence: **the sharer sends each
frame exactly once, regardless of how many people watch.**

## The five architecture points, and where each one stands

| # | Point | Status after M16 |
|---|-------|------------------|
| 1 | Peer-elected forwarding tree (a *peer role*, not an SFU server) | **Deferred** — seam left (see below) |
| 2 | Unidirectional stream transport, no per-frame acks, per-peer latest-wins queues | **Done** for video |
| 3 | Weak-machine encoding: screen-content codec, 5–15 fps, static-frame awareness | **Done pragmatically** (software OpenH264; temporal layers deferred) |
| 4 | Viewer→sharer feedback loop + forwarder keyframe cache | **Partially done** — implicit keyframe demand only; explicit feedback deferred |
| 5 | NAT traversal (relay / dcutr / autonat) | **Done** (M18) — see below |

## What M16 implements

### Transport (point 2)

- New protocol `/mikall/media-stream/1` in `mikall-net`: a unidirectional
  libp2p substream per (sender → viewer, kind), via `libp2p-stream`'s
  control API. Stream header = call id (16 B) ‖ kind (1 B); then
  length-prefixed sealed frames. **No acks, ever** — the old
  `/mikall/media/1` request-response-with-ack path was the wrong primitive
  for real-time media.
- Streams are opened and read entirely *outside* the swarm command loop
  (`libp2p_stream::Control` is its own channel to the connection), so
  video can never head-of-line-block DMs, dials, or signaling — and a
  stalled viewer's stream backpressure lands in that viewer's lane only.
- Per-viewer lanes (`VideoSendQueue` in `mikall-media`): bounded queue,
  and because H.264 delta frames chain, overflow drops the *run* — clear
  everything, demand a fresh keyframe — never a single frame from the
  middle. A stale screen frame is worse than a skipped one.
- **Voice still rides the old path** this milestone: migrating it too
  would have ballooned scope, and voice tolerates the ack path at 50
  frames/s on LAN. The new transport already carries a `kind` byte and an
  `Audio` variant of `MediaStreamKind`, so moving voice is a transport
  swap in the voice engine, not a redesign.

### Capture + encode (point 3, pragmatic v1)

- Capture: `scap` (ScreenCaptureKit on macOS) behind the `hardware-video`
  feature of `mikall-media` — same ports-and-adapters discipline as
  `hardware-audio`; the entire pipeline runs under tests with fake codecs
  and no hardware. Capture is configured to **≤1280 wide BGRA at 10 fps**;
  ScreenCaptureKit suppresses unchanged frames at the source and the
  adapter re-emits the last picture at ~1 Hz as a heartbeat.
- First capture triggers the macOS Screen Recording permission prompt.
  Denial is honest: a `MediaAlert::ScreenCaptureUnavailable` danger alarm
  ("System Settings → Privacy & Security → Screen Recording") and the
  share stays signaling-only. Nothing crashes, nothing pretends.
- Encoder: **software OpenH264** (`openh264` crate, bundled Cisco encoder)
  in `ScreenContentRealTime` mode. Justification against the weak-machine
  constraint: at 1280-wide/10 fps screen content the measured encode cost
  is milliseconds per frame and ~350 kbps on the wire (typical desktop
  activity; 1.5 Mbps ceiling), the crate needs no OS frameworks, and it
  decodes on every platform a viewer runs. VideoToolbox hardware H.264 is
  the planned upgrade *behind the same `VideoEncoder`/`VideoDecoder`
  ports* when higher rates justify the platform glue. Temporal layering
  (droppable every-other-frame) is deferred with the forwarding tree that
  would exploit it.
- Static frames cost ~nothing three times over: ScreenCaptureKit skips
  them, the `StaticFrameGate` drops exact repeats before the encoder, and
  OpenH264's own skip handling covers the rest.
- Frames ride the **existing sealed-frame protocol**: 18-byte
  authenticated header (`MediaKind::Video`, end-of-picture flag, keyframe
  flag) + ChaCha20-Poly1305 payload under the per-call `CallKey`, nonce =
  ssrc ‖ counter with a video-distinct ssrc. Encoded once, **sealed
  once** — every lane ships the same `Arc`'d bytes.

### Decode + render

- Per-sender receive lanes keyed (sender, kind): video has its own
  per-call tap beside the audio tap, so interleaved voice and video never
  cross (integration-tested byte-for-byte). Streams are ordered per
  sender, so there is no video jitter buffer — gate on the first
  keyframe, decode, present.
- The GUI viewer pane renders the live decoded picture (iced `image`
  updated per frame — fine at 10 fps). The honest placeholder remains
  *only* while no frame has arrived yet; the sharer keeps their "you are
  sharing" state and never gets a self-viewer.

### Late joiners (the M16 slice of point 4)

A new viewer's lane starts in "awaiting keyframe" state; the encode loop
sees the demand and forces a single IDR that every lane shares. The same
mechanism recovers a lane after a dropped run or a stream reopen. This is
the *implicit* feedback loop; the explicit one is deferred (below).

## What is deferred, and the seams left for it

- **Forwarding tree (point 1).** The fan-out is isolated behind exactly
  one interface: `VideoFanout` in `mikall-media` — the sharer's pipeline
  calls `broadcast` once per frame and never knows who is on the other
  end. Electing 1–2 forwarders means calling `VideoFanout::set_peers`
  with the forwarders instead of all viewers; capture, encode, and seal
  are untouched. Sealed bytes are relayable as-is: forwarders never need
  the call key to relay (AEAD is end-to-end), and the keyframe flag they
  need for caching is in the *authenticated plaintext header*. The
  domain-model work (publisher/subscriber roster roles, election scoring,
  raising the viewer cap past the 8-voice mesh invariant) is that
  milestone's core.
- **Keyframe cache.** Today the sharer re-encodes an IDR per join (cheap
  at 10 fps, wrong at scale). The cache belongs beside the lane queues in
  `VideoFanout` — on forwarders once they exist — holding the last sealed
  keyframe for instant late-join pictures without touching the sharer.
- **Explicit feedback (point 4).** Viewers do not yet report
  received-rate/loss, and the encoder does not adapt bitrate. The
  signaling idiom for it exists (`CallAction`, the `Mute` pattern); it was
  deliberately not added until there is an adaptation policy to serve.
- **NAT traversal (point 5) — done in M18.** The swarm now composes the
  stock rust-libp2p behaviours: autonat v1 (every node probes its own
  reachability *and* serves probes for others), the circuit-relay v2
  service (every mikall node donates modest, capped relay capacity by
  default — the peer-run relay model), the relay client + `/p2p-circuit`
  transport (a probed-private node automatically reserves a slot on a
  connected public peer and publishes the resulting circuit address in
  its listen-address list — the settings → network pane shows it with
  zero new UI), and DCUtR hole punching to upgrade relayed connections to
  direct ones. Relay limits are deliberately tight (8 reservations, 8
  circuits, 10 min / 8 MiB per direction per circuit), so media over a relay is a
  bridge until DCUtR lands, not a plan — see `docs/protocol.md` for the
  numbers and the honest failure mode. Publicly-reachable peers double as
  forwarder candidates for point 1, exactly as intended.
- **Voice on streams.** See transport section above.

## Honest v1 constraints

- Software H.264, one spatial/temporal layer, 10 fps, ≤1280 wide.
- Single-hop fan-out: the sharer still uplinks once *per viewer* (sealed
  once, sent N−1 times). Fine to the 8-seat voice cap; the forwarding
  tree is what changes the scaling law beyond it.
- Frames go to every call peer whether or not they opened the viewer
  pane (viewers decode only what they watch; suppressing unwatched sends
  is part of the subscriber-role work in point 1).
- A fully static screen can delay capture-thread shutdown until the next
  screen change (ScreenCaptureKit only wakes the thread on frames); in
  practice stopping a share repaints the screen and releases it
  immediately — the macOS capture indicator goes away on stop.
