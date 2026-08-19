//! Call audio wiring: watches call lifecycle events and runs the real
//! audio pipelines of `mikall-media` for every call we are actively in.
//!
//! The engine itself lives behind the `hardware-audio` feature (it opens
//! real devices); [`MediaAlert`] is always available so frontends can
//! subscribe unconditionally. Device trouble is honest, never fatal: a
//! missing or denied microphone raises an alert and the call continues
//! receive-only.

use mikall_domain::calls::CallId;

/// Device trouble surfaced to frontends via
/// [`crate::NodeHandle::subscribe_media_alerts`].
#[derive(Debug, Clone)]
pub enum MediaAlert {
    /// The microphone could not be opened — the call continues
    /// receive-only (peers hear nothing from us).
    MicUnavailable { call: CallId, detail: String },
    /// The output device could not be opened — peers still hear us, we
    /// hear nobody.
    SpeakerUnavailable { call: CallId, detail: String },
}

#[cfg(feature = "hardware-audio")]
pub(crate) use engine::spawn_voice_engine;

#[cfg(feature = "hardware-audio")]
mod engine {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use tokio::sync::broadcast;

    use mikall_app::events::{AppEvent, EventBus};
    use mikall_app::ports::MediaTransport;
    use mikall_app::services::{CallService, CallSnapshot, IdentityService};
    use mikall_domain::calls::{CallEvent, CallId, CallPhase};
    use mikall_domain::shared::IdentityId;
    use mikall_domain::DomainEvent;
    use mikall_media::engine::{
        run_receiver, run_sender, AudioCodec, SenderControl, FRAME_SAMPLES,
    };
    use mikall_media::frame::CallKey;
    use mikall_media::hardware::{CpalMic, CpalSpeaker};
    use mikall_media::opus::OpusCodec;

    use super::MediaAlert;

    /// One call's live audio: device keepalives and pipeline tasks.
    /// Dropping the mic closes the frame channel, which ends the sender
    /// loop; the receiver is aborted explicitly (its tap may outlive us).
    struct Session {
        control: Arc<SenderControl>,
        _mic: Option<CpalMic>,
        _speaker: Option<CpalSpeaker>,
        sender: Option<tokio::task::JoinHandle<u64>>,
        receiver: Option<tokio::task::JoinHandle<()>>,
    }

    impl Drop for Session {
        fn drop(&mut self) {
            if let Some(sender) = self.sender.take() {
                sender.abort();
            }
            if let Some(receiver) = self.receiver.take() {
                receiver.abort();
            }
        }
    }

    /// Decoder of last resort when libopus refuses to initialize: plays
    /// silence instead of pretending, so the failure stays audible as
    /// "that peer is silent", not garbage noise.
    #[derive(Debug, Default)]
    struct SilenceDecoder;

    impl AudioCodec for SilenceDecoder {
        fn encode(&mut self, _pcm: &[i16]) -> Vec<u8> {
            Vec::new()
        }

        fn decode(&mut self, _packet: Option<&[u8]>) -> Vec<i16> {
            vec![0; FRAME_SAMPLES]
        }
    }

    fn opus_decoder() -> Box<dyn AudioCodec> {
        match OpusCodec::new() {
            Ok(codec) => Box::new(codec),
            Err(error) => {
                tracing::error!(%error, "voice: opus decoder init failed, peer will be silent");
                Box::new(SilenceDecoder)
            }
        }
    }

    /// Watch the event bus and run audio for every call we are in. One
    /// task per node; aborted on shutdown like the swarm loop.
    pub(crate) fn spawn_voice_engine(
        identity: Arc<IdentityService>,
        calls: Arc<CallService>,
        media: Arc<dyn MediaTransport>,
        bus: EventBus,
        alerts: broadcast::Sender<MediaAlert>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut sessions: BTreeMap<CallId, Session> = BTreeMap::new();
            let mut rx = bus.subscribe();
            loop {
                let event = match rx.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let AppEvent::Domain(DomainEvent::Calls(call_event)) = event else {
                    continue;
                };
                let call = event_call_id(&call_event);
                reconcile(call, &mut sessions, &identity, &calls, &media, &alerts).await;
            }
        })
    }

    fn event_call_id(event: &CallEvent) -> CallId {
        match event {
            CallEvent::CallOffered { call, .. }
            | CallEvent::CallOpened { call, .. }
            | CallEvent::CallInvited { call, .. }
            | CallEvent::CallAccepted { call, .. }
            | CallEvent::CallDeclined { call, .. }
            | CallEvent::ParticipantJoined { call, .. }
            | CallEvent::ParticipantLeft { call, .. }
            | CallEvent::ScreenShareStarted { call, .. }
            | CallEvent::ScreenShareStopped { call, .. }
            | CallEvent::MicMuted { call, .. }
            | CallEvent::MicUnmuted { call, .. }
            | CallEvent::CallEnded { call, .. } => *call,
        }
    }

    /// Drive the session set toward what the aggregate says: audio runs
    /// exactly while the call is Active, we are seated, and someone else
    /// is too. Every call event lands here, so mute flips and roster
    /// changes reach the running pipelines without restarts.
    async fn reconcile(
        call: CallId,
        sessions: &mut BTreeMap<CallId, Session>,
        identity: &Arc<IdentityService>,
        calls: &Arc<CallService>,
        media: &Arc<dyn MediaTransport>,
        alerts: &broadcast::Sender<MediaAlert>,
    ) {
        let me = identity.local_id();
        let snapshot = calls.snapshot(call).await.ok();
        let (live, peers, muted) = match &snapshot {
            None => (false, Vec::new(), false),
            Some(snapshot) => {
                let peers: Vec<IdentityId> = snapshot
                    .participants
                    .iter()
                    .map(|(who, _)| *who)
                    .filter(|who| *who != me)
                    .collect();
                let seated = snapshot.participants.iter().any(|(who, _)| *who == me);
                let muted = snapshot
                    .participants
                    .iter()
                    .find(|(who, _)| *who == me)
                    .is_some_and(|(_, media)| media.mic_muted);
                let live =
                    matches!(snapshot.phase, CallPhase::Active) && seated && !peers.is_empty();
                (live, peers, muted)
            }
        };
        if let Some(session) = sessions.get(&call) {
            if live {
                session.control.set_peers(peers);
                session.control.set_muted(muted);
            } else {
                tracing::info!(%call, "voice: stopping audio session");
                sessions.remove(&call);
            }
            return;
        }
        if !live {
            return;
        }
        let Some(snapshot) = snapshot else { return };
        if let Some(session) =
            start_session(call, &snapshot, peers, muted, me, calls, media, alerts).await
        {
            sessions.insert(call, session);
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_session(
        call: CallId,
        _snapshot: &CallSnapshot,
        peers: Vec<IdentityId>,
        muted: bool,
        me: IdentityId,
        calls: &Arc<CallService>,
        media: &Arc<dyn MediaTransport>,
        alerts: &broadcast::Sender<MediaAlert>,
    ) -> Option<Session> {
        let Some(key_bytes) = calls.media_key(call).await else {
            tracing::warn!(%call, "voice: no media key for active call, audio disabled");
            return None;
        };
        // Distinct per sender within the call (AEAD nonce = ssrc ‖ counter):
        // derived from our identity, as documented in mikall-media.
        let id_bytes = me.as_bytes();
        let ssrc = u32::from_be_bytes([id_bytes[0], id_bytes[1], id_bytes[2], id_bytes[3]]);
        tracing::info!(%call, peers = peers.len(), muted, ssrc, "voice: starting audio session");

        let control = SenderControl::new(peers);
        control.set_muted(muted);

        // Speakers first: playback works even when the mic does not.
        let (speaker, sink) = match CpalSpeaker::start() {
            Ok((speaker, sink)) => (Some(speaker), Some(sink)),
            Err(error) => {
                tracing::warn!(%call, %error, "voice: speaker unavailable");
                let _ = alerts.send(MediaAlert::SpeakerUnavailable {
                    call,
                    detail: error.to_string(),
                });
                (None, None)
            }
        };
        let receiver = sink.map(|mut sink| {
            let tap_calls = Arc::clone(calls);
            let key = CallKey::new(key_bytes);
            tokio::spawn(async move {
                let tap = tap_calls.media_tap(call).await;
                let stats = run_receiver(tap, &key, opus_decoder, &mut sink, None).await;
                tracing::info!(%call, ?stats, "voice: receiver finished");
            })
        });

        // Mic: a denial (macOS permission, no device) is an alert, and the
        // session continues receive-only.
        let (mic, sender) = match CpalMic::start() {
            Ok((mic, frames)) => match OpusCodec::new() {
                Ok(codec) => {
                    let key = CallKey::new(key_bytes);
                    let transport = Arc::clone(media);
                    let sender_control = Arc::clone(&control);
                    let task = tokio::spawn(async move {
                        run_sender(
                            transport,
                            call,
                            &key,
                            ssrc,
                            sender_control,
                            frames,
                            Box::new(codec),
                        )
                        .await
                    });
                    (Some(mic), Some(task))
                }
                Err(error) => {
                    tracing::warn!(%call, %error, "voice: opus encoder init failed");
                    let _ = alerts.send(MediaAlert::MicUnavailable {
                        call,
                        detail: error.to_string(),
                    });
                    (None, None)
                }
            },
            Err(error) => {
                tracing::warn!(%call, %error, "voice: microphone unavailable");
                let _ = alerts.send(MediaAlert::MicUnavailable {
                    call,
                    detail: error.to_string(),
                });
                (None, None)
            }
        };

        Some(Session {
            control,
            _mic: mic,
            _speaker: speaker,
            sender,
            receiver,
        })
    }
}
