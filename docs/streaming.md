# Streaming: how screen-share video moves

This document adapts the screen-sharing architecture consult into the
project's plan of record. The problem it solves: p2p full-mesh media makes
the *sharer* pay for every viewer — N−1 uplinks and encodes on exactly the
machine least able to afford them (weak CPU, slow uplink, 10+ viewers).
The goal of the whole design is one sentence: **the sharer sends each
frame exactly once, regardless of how many people watch.**

## The five architecture points, and where each one stands

| # | Point | Status |
|---|-------|--------|
| 1 | Peer-elected forwarding tree (a *peer role*, not an SFU server) | **Done with caveats** (M17) — depth-2 tree, one forwarder, deterministic v1 election; see below |
| 2 | Unidirectional stream transport, no per-frame acks, per-peer latest-wins queues | **Done** for video |
| 3 | Weak-machine encoding: screen-content codec, 5–15 fps, static-frame awareness | **Done pragmatically** (software OpenH264; temporal layers deferred) |
| 4 | Viewer→sharer feedback loop + forwarder keyframe cache | **Done with caveats** (M17) — coarse 2 s reports, loss-based bitrate steps, keyframe-run cache on forwarders |
| 5 | NAT traversal (relay / dcutr / autonat) | **Done** (M18, a parallel milestone) — see below |

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
the *implicit* feedback loop; M17 adds the explicit one (below) and moves
the late-join burden onto the forwarder's keyframe cache.

## What M17 implements (points 1 + 4)

### The forwarding tree (point 1): a peer role, never a server

- **Domain.** The `Call` aggregate owns forwarding: per-sharer elected
  forwarder entries that *cannot* be invalid — a forwarder that isn't
  seated, a sharer forwarding their own stream, or a forwarder for a
  non-share are all unrepresentable (`elect_forwarder` refuses them, and
  `leave`/`set_screen_sharing(false)` clear entries the moment either
  party departs or the share stops, emitting `ForwarderCleared`).
  Election *policy* is a domain-service seam (`ForwarderStrategy`): v1 is
  `LowestSeatedIdentity` — **the lowest identity among seated non-sharer
  peers**, deterministic and re-derivable from roster state alone, and
  only when ≥ 2 candidates exist (with one viewer, a forwarder is pure
  overhead — direct fan-out costs the sharer the same single uplink).
  Real scoring (uplink bandwidth, CPU headroom, public reachability — the
  M18 autonat verdicts are the natural input) replaces the implementation
  behind the same trait. An elected forwarder *keeps* the role while it
  stays seated and reachable: stability beats optimality, nobody swaps a
  working forwarder because a lower identity joined.
- **Election runs on the sharer's node** (`CallService::
  reconcile_forwarder`), whose roster view is authoritative for its own
  share; it re-runs on every call event and on a 1 s watchdog. Transport
  failures feed it: a forwarder whose lane fails ~3 consecutive
  opens/sends counts as unreachable and is re-elected away — the v1
  stand-in for reachability scoring.
- **Signaling rides in-band** on the existing media streams as sealed
  `MediaKind::Control` frames under the call key (`ControlMessage`:
  assign / revoke / ack / feedback / keyframe-request) — exactly as
  authenticated as the media itself, zero new wire protocols. The
  `CallAction`-envelope form the spec sketched needs a wire-DTO addition
  in `mikall-net`, which was frozen under the parallel NAT milestone;
  moving these five messages onto signed envelopes later is a mechanical
  transport swap behind `CallService`.
- **Sharer side:** when a forwarder is in force, `VideoFanout::set_peers`
  targets *only the forwarder* — capture, encode, and seal never learn
  the difference, and the per-second `video tx` log's `tx_lanes` shows 1
  regardless of viewer count. No eligible forwarder (2-party call, or the
  forwarder just died) → `set_peers(all viewers)`, byte-for-byte the M16
  behavior. A dying forwarder never ends a share: re-election plus
  retarget costs at worst a brief stall, and fresh lanes recover through
  the existing awaiting-keyframe machinery.
- **Forwarder side:** the per-call *video pump* (`mikall-media::relay`)
  owns the inbound tap and, while assigned, repeats the sharer's sealed
  frames verbatim into its own relay `VideoFanout` toward the viewer set.
  **The relay path never decrypts**: it reads only the plaintext
  authenticated header (`peek_header`) — the keyframe flag travels as
  AAD precisely so a relay can trust it without the key. The tests pin
  this down by relaying frames sealed under a key the pump does not hold.
  The forwarder also remains a full viewer (it decodes its own copy), and
  viewers attribute relayed frames to the *sharer* by ssrc, rendering
  "via <forwarder>" in the viewer pane.
- **Keyframe cache (the forwarder half of point 4).** Beside the relay
  fan-out's lane queues sits a `KeyframeCache` holding the last complete
  keyframe *run* (keyframe + every delta since — H.264 deltas chain, so
  anything less would decode to garbage; byte-capped at 4 MiB, an
  overgrown run is discarded whole). A late-joining viewer's fresh lane
  is primed from the cache and sees a picture immediately — the sharer
  re-encodes nothing. Only when the cache cannot serve does the forwarder
  send a `KeyframeRequest` and the sharer force one IDR.

### The feedback loop (point 4): viewers report, the sharer adapts

- **Reports go direct to the sharer**, not aggregated by the forwarder:
  at the 8-seat mesh cap that is ≤ 7 tiny sealed messages every 2 s; it
  works identically with no forwarder (2-party fallback) and survives the
  forwarder dying mid-share. Forwarder aggregation earns its complexity
  only with subscribe-only audiences (below).
- **What a viewer measures:** per 2 s window, `received` = sealed video
  frames that arrived, `lost` = header-counter gaps (streams are ordered
  and reliable per hop, so a gap is a run dropped at some sender lane —
  the congestion signal). Measured on plaintext headers, no decode
  dependency; empty windows are silence and report nothing.
- **The control law** (`BitrateController`, deliberately simple and
  stable — worst viewer governs):
  - any report with **≥ 5 % loss** steps the encoder target down to
    **70 %**, at most once per **4 s** cooldown, floor **200 kbps**;
  - the target steps up to **125 %** only after **10 s** in which *every*
    report was ≤ 1 % loss (any bad window resets the clock), same 10 s
    cooldown, ceiling **1.5 Mbps** (the encoder default);
  - actuator: the OpenH264 encoder is rebuilt at the new bitrate on the
    next frame (the crate has no safe runtime setter); the rebuild's
    opening IDR doubles as the resync every lane accepts.

## What is deferred, and the seams left for it

- **Viewers are still full call participants.** The forwarding tree
  changes who *carries* the pixels, not who may watch: every viewer holds
  a seat, so the mesh cap of 8 still bounds a share's audience. The next
  architectural step on this axis is the **subscribe-only audience** —
  watchers who hold the call key and a stream lane but no roster seat (no
  voice mesh cost, no cap pressure); that is the milestone where the
  domain grows a publisher/subscriber split beside the seated roster,
  forwarder *scoring* starts to matter (aggregate audience uplink), the
  tree may need depth > 2 and more than one forwarder, and
  forwarder-side feedback aggregation earns its place.
- **Temporal layering** (droppable every-other-frame) still waits; with
  the tree in place, forwarders are now positioned to exploit it by
  thinning streams per slow viewer instead of dropping whole runs.
- **NAT traversal (point 5) — done in M18, a parallel milestone.** The swarm now composes the
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
- With ≥ 2 other seats the sharer uplinks each frame **once, to the
  forwarder** — sharer cost is constant in viewer count; the forwarder
  pays the N−1 fan-out (it was elected as the peer that can). In a
  2-party call the sharer sends directly, which is the same single
  uplink.
- The audience is still capped at the 8-seat mesh (viewers are seated
  participants); subscribe-only watchers beyond the cap are the next
  step, above.
- Frames go to every call peer whether or not they opened the viewer
  pane (viewers decode only what they watch; suppressing unwatched sends
  is part of the same subscriber-role work).
- Election trusts the transport's failure counters for liveness; a
  forwarder that is up but *selectively* not forwarding is undetected
  until viewers' loss reports say so (and even then v1 only adapts
  bitrate). Malicious-forwarder handling rides on feedback-driven
  re-election, later.
- A fully static screen can delay capture-thread shutdown until the next
  screen change (ScreenCaptureKit only wakes the thread on frames); in
  practice stopping a share repaints the screen and releases it
  immediately — the macOS capture indicator goes away on stop.
