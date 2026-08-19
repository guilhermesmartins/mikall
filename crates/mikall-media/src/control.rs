//! In-band call control for the forwarding tree and the feedback loop.
//!
//! Control messages ride the *existing* unidirectional media streams as
//! sealed frames of [`MediaKind::Control`] under the per-call key — the
//! same authenticity as media itself, zero new wire protocols. (The
//! `CallAction`-envelope form of this signaling needs a wire-DTO addition
//! in `mikall-net`, which is frozen this milestone; moving these messages
//! onto signed envelopes later is a mechanical transport swap.)
//!
//! Who says what:
//! - sharer → forwarder: [`ControlMessage::ForwarderAssign`] (the viewer
//!   set to relay to) and [`ControlMessage::ForwarderRevoke`];
//! - forwarder → sharer: [`ControlMessage::ForwarderAck`] and
//!   [`ControlMessage::KeyframeRequest`] (a primed lane needs an IDR);
//! - viewer → sharer: [`ControlMessage::Feedback`] every couple of
//!   seconds — coarse received/lost counts the sharer's
//!   [`BitrateController`] turns into encoder bitrate steps.
//!
//! Feedback goes **direct to the sharer**, not aggregated by the
//! forwarder: at the 8-seat mesh cap that is at most 7 tiny messages
//! every 2 s, it works identically when there is no forwarder (2-party
//! fallback), and it survives the forwarder dying mid-share. Forwarder
//! aggregation becomes worthwhile only with subscribe-only audiences.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use mikall_app::ports::{MediaSendStream, MediaStreamKind, MediaStreamTransport};
use mikall_domain::calls::CallId;
use mikall_domain::shared::IdentityId;

use crate::frame::{seal, CallKey, FrameHeader, MediaKind};

/// The control stream's ssrc is derived from the identity like audio and
/// video, XOR a distinct constant, so one call key never sees two streams
/// of the same sender share an AEAD nonce sequence.
pub fn control_ssrc_of(id: &IdentityId) -> u32 {
    let b = id.as_bytes();
    u32::from_be_bytes([b[0], b[1], b[2], b[3]]) ^ 0xA5C3_17E9
}

/// One viewer's coarse reception report over its last window (~2 s).
/// `received` counts sealed video frames that arrived; `lost` counts
/// header-counter gaps — frames the sender or a relay dropped upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FeedbackReport {
    pub received: u32,
    pub lost: u32,
}

impl FeedbackReport {
    /// Fraction of the window's frames that never arrived (0 when the
    /// window was empty — an empty window is silence, not loss).
    pub fn loss_ratio(&self) -> f32 {
        let total = self.received + self.lost;
        if total == 0 {
            0.0
        } else {
            self.lost as f32 / total as f32
        }
    }
}

/// Everything that travels as a sealed control frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlMessage {
    /// Sharer → forwarder: relay my sealed video frames to `viewers`.
    /// Re-sent whenever the viewer set changes; idempotent.
    ForwarderAssign { viewers: Vec<IdentityId> },
    /// Sharer → forwarder: stand down (share stopping or re-election).
    ForwarderRevoke,
    /// Forwarder → sharer: assignment received, relay running.
    ForwarderAck,
    /// Viewer → sharer: reception report for the sharer's stream.
    Feedback(FeedbackReport),
    /// Forwarder → sharer: a relay lane awaits a keyframe the cache
    /// could not serve — force one IDR.
    KeyframeRequest,
}

const CONTROL_VERSION: u8 = 1;
const TAG_ASSIGN: u8 = 1;
const TAG_REVOKE: u8 = 2;
const TAG_ACK: u8 = 3;
const TAG_FEEDBACK: u8 = 4;
const TAG_KEYFRAME_REQUEST: u8 = 5;

/// The mesh cap bounds any honest viewer list; anything bigger is
/// hostile or corrupt.
const MAX_VIEWERS: usize = 16;

impl ControlMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![CONTROL_VERSION];
        match self {
            ControlMessage::ForwarderAssign { viewers } => {
                out.push(TAG_ASSIGN);
                out.push(viewers.len().min(MAX_VIEWERS) as u8);
                for viewer in viewers.iter().take(MAX_VIEWERS) {
                    out.extend_from_slice(viewer.as_bytes());
                }
            }
            ControlMessage::ForwarderRevoke => out.push(TAG_REVOKE),
            ControlMessage::ForwarderAck => out.push(TAG_ACK),
            ControlMessage::Feedback(report) => {
                out.push(TAG_FEEDBACK);
                out.extend_from_slice(&report.received.to_be_bytes());
                out.extend_from_slice(&report.lost.to_be_bytes());
            }
            ControlMessage::KeyframeRequest => out.push(TAG_KEYFRAME_REQUEST),
        }
        out
    }

    /// Decode a control payload (already authenticated by the AEAD open).
    /// `None` for unknown versions/tags — a newer peer's message is
    /// ignored, never an error that kills the stream.
    pub fn decode(payload: &[u8]) -> Option<ControlMessage> {
        let [CONTROL_VERSION, tag, rest @ ..] = payload else {
            return None;
        };
        match *tag {
            TAG_ASSIGN => {
                let (&count, ids) = rest.split_first()?;
                let count = count as usize;
                if count > MAX_VIEWERS || ids.len() != count * 32 {
                    return None;
                }
                let viewers = ids
                    .chunks_exact(32)
                    .map(|chunk| {
                        let mut bytes = [0u8; 32];
                        bytes.copy_from_slice(chunk);
                        IdentityId::from_bytes(bytes)
                    })
                    .collect();
                Some(ControlMessage::ForwarderAssign { viewers })
            }
            TAG_REVOKE => rest.is_empty().then_some(ControlMessage::ForwarderRevoke),
            TAG_ACK => rest.is_empty().then_some(ControlMessage::ForwarderAck),
            TAG_FEEDBACK => {
                if rest.len() != 8 {
                    return None;
                }
                let received = u32::from_be_bytes(rest[..4].try_into().ok()?);
                let lost = u32::from_be_bytes(rest[4..].try_into().ok()?);
                Some(ControlMessage::Feedback(FeedbackReport { received, lost }))
            }
            TAG_KEYFRAME_REQUEST => rest.is_empty().then_some(ControlMessage::KeyframeRequest),
            _ => None,
        }
    }
}

/// Sends sealed control frames to call peers over the media-stream
/// transport, one lazily opened lane per recipient. One shared counter
/// across all lanes: the AEAD nonce is `ssrc ‖ counter`, and this sender
/// has exactly one control ssrc, so per-lane counters would reuse nonces.
pub struct ControlPlane {
    transport: Arc<dyn MediaStreamTransport>,
    call: CallId,
    key: CallKey,
    ssrc: u32,
    counter: AtomicU64,
    lanes: AsyncMutex<BTreeMap<IdentityId, Box<dyn MediaSendStream>>>,
}

impl std::fmt::Debug for ControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlane")
            .field("call", &self.call)
            .finish_non_exhaustive()
    }
}

impl ControlPlane {
    pub fn new(
        transport: Arc<dyn MediaStreamTransport>,
        call: CallId,
        key: CallKey,
        local: &IdentityId,
    ) -> Arc<Self> {
        Arc::new(ControlPlane {
            transport,
            call,
            key,
            ssrc: control_ssrc_of(local),
            counter: AtomicU64::new(0),
            lanes: AsyncMutex::new(BTreeMap::new()),
        })
    }

    /// Seal and send one control message. Best effort: a dead lane is
    /// dropped and reopened once; `false` means the peer is unreachable
    /// right now (the caller's watchdog retries, control is periodic).
    /// Every await is deadline-bounded — a silently dead peer blocks
    /// writes forever (full flow-control window, no error), and a hung
    /// control send must never stall the engine that drives it.
    pub async fn send(&self, to: IdentityId, message: &ControlMessage) -> bool {
        const OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
        const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
        let payload = message.encode();
        let header = FrameHeader {
            counter: self.counter.fetch_add(1, Ordering::Relaxed),
            ts: 0,
            ssrc: self.ssrc,
            kind: MediaKind::Control,
            flags: 0,
        };
        let Ok(sealed) = seal(&self.key, header, &payload) else {
            return false;
        };
        let mut lanes = self.lanes.lock().await;
        for _attempt in 0..2 {
            if let std::collections::btree_map::Entry::Vacant(slot) = lanes.entry(to) {
                match tokio::time::timeout(
                    OPEN_TIMEOUT,
                    self.transport
                        .open_stream(to, self.call, MediaStreamKind::Video),
                )
                .await
                {
                    Ok(Ok(stream)) => {
                        slot.insert(stream);
                    }
                    Ok(Err(error)) => {
                        tracing::debug!(%to, call = %self.call, %error, "control lane open failed");
                        return false;
                    }
                    Err(_) => {
                        tracing::debug!(%to, call = %self.call, "control lane open timed out");
                        return false;
                    }
                }
            }
            if let Some(stream) = lanes.get_mut(&to) {
                match tokio::time::timeout(SEND_TIMEOUT, stream.send(&sealed)).await {
                    Ok(Ok(())) => return true,
                    Ok(Err(_)) | Err(_) => {
                        // Stale or stalled substream (peer died or
                        // restarted the call path): drop it and retry
                        // once on a fresh one.
                        lanes.remove(&to);
                    }
                }
            }
        }
        false
    }
}

/// Loss-based single-encode adaptation — the sharer's side of the
/// feedback loop. Deliberately simple and stable (docs/streaming.md
/// documents the law):
///
/// - any report with ≥ [`LOSS_STEP_DOWN`] loss steps the target down to
///   70%, at most once per [`STEP_DOWN_COOLDOWN_MS`], never below the
///   floor;
/// - the target steps up to 125% only after [`STEP_UP_AFTER_CLEAN_MS`]
///   with *every* report at ≤ [`LOSS_CLEAN`] loss (any viewer's bad
///   window resets the clock — the worst viewer governs), never above
///   the ceiling;
/// - empty windows are silence, not loss: they never move the target.
#[derive(Debug, Clone)]
pub struct BitrateController {
    target: u32,
    floor: u32,
    ceiling: u32,
    /// `None` until the first step: a fresh controller may react to the
    /// very first lossy report immediately.
    last_step_ms: Option<u64>,
    last_unclean_ms: u64,
}

/// Loss fraction that triggers a step down (5%).
pub const LOSS_STEP_DOWN: f32 = 0.05;
/// Loss fraction still considered clean (1%).
pub const LOSS_CLEAN: f32 = 0.01;
/// Minimum spacing between downward steps.
pub const STEP_DOWN_COOLDOWN_MS: u64 = 4_000;
/// Clean time required before an upward step (also its cooldown).
pub const STEP_UP_AFTER_CLEAN_MS: u64 = 10_000;

impl BitrateController {
    pub fn new(start: u32, floor: u32, ceiling: u32) -> Self {
        let floor = floor.min(ceiling);
        BitrateController {
            target: start.clamp(floor, ceiling),
            floor,
            ceiling,
            last_step_ms: None,
            last_unclean_ms: 0,
        }
    }

    fn stepped_within(&self, now_ms: u64, window_ms: u64) -> bool {
        self.last_step_ms
            .is_some_and(|at| now_ms.saturating_sub(at) < window_ms)
    }

    pub fn target(&self) -> u32 {
        self.target
    }

    /// Feed one viewer report. `Some(new_target)` when the target moved.
    pub fn on_report(&mut self, now_ms: u64, report: &FeedbackReport) -> Option<u32> {
        let total = report.received + report.lost;
        if total == 0 {
            return None;
        }
        let loss = report.loss_ratio();
        if loss > LOSS_CLEAN {
            self.last_unclean_ms = now_ms;
        }
        if loss >= LOSS_STEP_DOWN {
            if self.target > self.floor && !self.stepped_within(now_ms, STEP_DOWN_COOLDOWN_MS) {
                self.target = ((u64::from(self.target) * 7 / 10) as u32).max(self.floor);
                self.last_step_ms = Some(now_ms);
                return Some(self.target);
            }
            return None;
        }
        if self.target < self.ceiling
            && now_ms.saturating_sub(self.last_unclean_ms) >= STEP_UP_AFTER_CLEAN_MS
            && !self.stepped_within(now_ms, STEP_UP_AFTER_CLEAN_MS)
        {
            self.target = ((u64::from(self.target) * 5 / 4) as u32).min(self.ceiling);
            self.last_step_ms = Some(now_ms);
            return Some(self.target);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn identity(n: u8) -> IdentityId {
        IdentityId::from_bytes([n; 32])
    }

    #[test]
    fn control_messages_roundtrip() {
        let messages = [
            ControlMessage::ForwarderAssign {
                viewers: vec![identity(3), identity(7)],
            },
            ControlMessage::ForwarderAssign { viewers: vec![] },
            ControlMessage::ForwarderRevoke,
            ControlMessage::ForwarderAck,
            ControlMessage::Feedback(FeedbackReport {
                received: 20,
                lost: 3,
            }),
            ControlMessage::KeyframeRequest,
        ];
        for message in messages {
            let decoded = ControlMessage::decode(&message.encode());
            assert_eq!(decoded, Some(message));
        }
    }

    #[test]
    fn malformed_control_is_none_never_a_panic() {
        assert_eq!(ControlMessage::decode(&[]), None);
        assert_eq!(ControlMessage::decode(&[9, TAG_ACK]), None); // bad version
        assert_eq!(ControlMessage::decode(&[CONTROL_VERSION, 99]), None); // bad tag
        assert_eq!(
            ControlMessage::decode(&[CONTROL_VERSION, TAG_ASSIGN, 2, 0]),
            None
        ); // short ids
        assert_eq!(
            ControlMessage::decode(&[CONTROL_VERSION, TAG_FEEDBACK, 1, 2, 3]),
            None
        );
        // A hostile viewer count is refused outright.
        let mut hostile = vec![CONTROL_VERSION, TAG_ASSIGN, 255];
        hostile.extend(std::iter::repeat_n(0, 255 * 32));
        assert_eq!(ControlMessage::decode(&hostile), None);
    }

    #[test]
    fn control_ssrc_is_distinct_from_audio_and_video() {
        let id = identity(0xAB);
        let b = id.as_bytes();
        let audio = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        let video = crate::video::video_ssrc_of(&id);
        let control = control_ssrc_of(&id);
        assert_ne!(control, audio);
        assert_ne!(control, video);
    }

    #[test]
    fn loss_steps_bitrate_down_with_a_cooldown() {
        let mut controller = BitrateController::new(1_500_000, 200_000, 1_500_000);
        let lossy = FeedbackReport {
            received: 18,
            lost: 2,
        }; // 10% loss
        assert_eq!(controller.on_report(0, &lossy), Some(1_050_000));
        // Cooldown holds: an immediate second bad report changes nothing.
        assert_eq!(controller.on_report(2_000, &lossy), None);
        assert_eq!(controller.target(), 1_050_000);
        // After the cooldown the next bad report steps again.
        assert_eq!(controller.on_report(4_100, &lossy), Some(735_000));
    }

    #[test]
    fn bitrate_never_leaves_the_floor_ceiling_band() {
        let mut controller = BitrateController::new(300_000, 200_000, 1_500_000);
        let lossy = FeedbackReport {
            received: 10,
            lost: 10,
        };
        let mut now = 0;
        for _ in 0..10 {
            let _ = controller.on_report(now, &lossy);
            now += STEP_DOWN_COOLDOWN_MS + 1;
        }
        assert_eq!(controller.target(), 200_000, "floor holds");
    }

    #[test]
    fn sustained_clean_windows_recover_bitrate() {
        let mut controller = BitrateController::new(1_500_000, 200_000, 1_500_000);
        let lossy = FeedbackReport {
            received: 10,
            lost: 5,
        };
        assert!(controller.on_report(1_000, &lossy).is_some());
        let clean = FeedbackReport {
            received: 20,
            lost: 0,
        };
        // Clean reports inside the 10 s window: no step up yet.
        assert_eq!(controller.on_report(5_000, &clean), None);
        assert_eq!(controller.on_report(9_000, &clean), None);
        // 10 s of clean air since the last unclean report: recover.
        let recovered = controller.on_report(11_500, &clean);
        assert_eq!(recovered, Some(1_312_500));
        // And the ceiling caps recovery.
        let later = controller.on_report(23_000, &clean);
        assert_eq!(later, Some(1_500_000));
        assert_eq!(controller.on_report(40_000, &clean), None);
    }

    #[test]
    fn empty_windows_and_mild_loss_hold_steady() {
        let mut controller = BitrateController::new(1_000_000, 200_000, 1_500_000);
        assert_eq!(
            controller.on_report(1_000, &FeedbackReport::default()),
            None
        );
        // 3% loss: below the step-down threshold, above clean — holds,
        // but resets the clean clock.
        let mild = FeedbackReport {
            received: 97,
            lost: 3,
        };
        assert_eq!(controller.on_report(2_000, &mild), None);
        assert_eq!(controller.target(), 1_000_000);
    }
}
