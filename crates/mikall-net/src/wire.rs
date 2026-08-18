//! Wire DTOs (CBOR) and the envelope signing/verification path.
//!
//! Nothing in here reaches the application layer unverified: the only way
//! bytes become domain-facing values is [`open_envelope`] /
//! [`open_dm`], each of which checks the Ed25519 signature *and* the
//! author-consistency and content-derived-id rules, then mints the
//! `Verified` attestation next to that check.

use serde::{Deserialize, Serialize};

use mikall_app::ports::{ChannelSignal, WireMessage};
use mikall_crypto::{derive_message_id, verify_signature, LocalKeys};
use mikall_domain::messaging::{MessageId, Verified};
use mikall_domain::shared::IdentityId;
use mikall_domain::transfer::{BlobHash, FileManifest, FileName};

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("malformed envelope: {0}")]
    Malformed(String),
    #[error("bad signature")]
    BadSignature,
    #[error("author mismatch inside envelope")]
    AuthorMismatch,
    #[error("message id does not match content")]
    IdMismatch,
}

/// Ceiling for any envelope we will parse (DoS bound; also enforced by
/// gossipsub's max transmit size).
pub const MAX_ENVELOPE_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct MsgDto {
    pub id: Vec<u8>,
    pub author: Vec<u8>,
    pub lamport: u64,
    pub parents: Vec<Vec<u8>>,
    pub body: String,
    pub ts_hint_ms: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SignalDto {
    Msg(MsgDto),
    Joined {
        who: Vec<u8>,
        nickname: Option<String>,
    },
    Left {
        who: Vec<u8>,
    },
    Topic {
        by: Vec<u8>,
        topic: String,
    },
    Role {
        by: Vec<u8>,
        target: Vec<u8>,
        op: bool,
    },
    Presence {
        who: Vec<u8>,
        away: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestDto {
    pub name: String,
    pub size: u64,
    pub root: Vec<u8>,
    pub chunk_hashes: Vec<Vec<u8>>,
}

/// Payload of the direct protocol: chat or a file offer.
#[derive(Debug, Serialize, Deserialize)]
pub enum DmPayloadDto {
    Chat(MsgDto),
    Offer(ManifestDto),
}

/// Decoded, verified direct payload.
#[derive(Debug)]
pub enum DmPayload {
    Chat(WireMessage),
    Offer(FileManifest),
}

fn manifest_dto(manifest: &FileManifest) -> ManifestDto {
    ManifestDto {
        name: manifest.name().as_str().to_owned(),
        size: manifest.size(),
        root: manifest.root().as_bytes().to_vec(),
        chunk_hashes: (0..manifest.chunk_count())
            .filter_map(|i| manifest.chunk_hash(i))
            .map(|h| h.as_bytes().to_vec())
            .collect(),
    }
}

fn parse_manifest(dto: ManifestDto) -> Result<FileManifest, WireError> {
    let name = FileName::parse(&dto.name).map_err(|e| WireError::Malformed(e.to_string()))?;
    let root = BlobHash::from_bytes(to_array32(&dto.root)?);
    let chunk_hashes = dto
        .chunk_hashes
        .iter()
        .map(|h| to_array32(h).map(BlobHash::from_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    // The domain constructor re-checks size/chunk-count invariants, so a
    // malformed manifest cannot cross this boundary.
    FileManifest::new(name, dto.size, root, chunk_hashes)
        .map_err(|e| WireError::Malformed(e.to_string()))
}

/// A signed envelope as carried on gossip topics and the DM protocol.
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedEnvelope {
    /// CBOR bytes of a [`SignalDto`] (channels) or [`MsgDto`] (DMs). The
    /// signature covers exactly these bytes — sign the bytes, never
    /// re-serialize-then-verify.
    pub payload: Vec<u8>,
    /// Ed25519 public key of the author (32 bytes).
    pub author: Vec<u8>,
    /// Ed25519 signature over `payload` (64 bytes).
    pub sig: Vec<u8>,
}

fn to_array32(bytes: &[u8]) -> Result<[u8; 32], WireError> {
    bytes
        .try_into()
        .map_err(|_| WireError::Malformed("expected 32 bytes".into()))
}

fn msg_dto(message: &WireMessage) -> MsgDto {
    MsgDto {
        id: message.id.as_bytes().to_vec(),
        author: message.author.as_bytes().to_vec(),
        lamport: message.lamport,
        parents: message
            .parents
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect(),
        body: message.body.clone(),
        ts_hint_ms: message.ts_hint_ms,
    }
}

fn signal_dto(signal: &ChannelSignal) -> SignalDto {
    match signal {
        ChannelSignal::Message(m) => SignalDto::Msg(msg_dto(m)),
        ChannelSignal::Joined { who, nickname } => SignalDto::Joined {
            who: who.as_bytes().to_vec(),
            nickname: nickname.clone(),
        },
        ChannelSignal::Left { who } => SignalDto::Left {
            who: who.as_bytes().to_vec(),
        },
        ChannelSignal::TopicChanged { by, topic } => SignalDto::Topic {
            by: by.as_bytes().to_vec(),
            topic: topic.clone(),
        },
        ChannelSignal::RoleChanged { by, target, op } => SignalDto::Role {
            by: by.as_bytes().to_vec(),
            target: target.as_bytes().to_vec(),
            op: *op,
        },
        ChannelSignal::Presence { who, away } => SignalDto::Presence {
            who: who.as_bytes().to_vec(),
            away: away.clone(),
        },
    }
}

fn parse_msg(dto: MsgDto) -> Result<WireMessage, WireError> {
    let author = IdentityId::from_bytes(to_array32(&dto.author)?);
    let id = MessageId::from_bytes(to_array32(&dto.id)?);
    let parents = dto
        .parents
        .iter()
        .map(|p| to_array32(p).map(MessageId::from_bytes))
        .collect::<Result<Vec<_>, _>>()?;
    // Content-derived id: recompute and refuse forgeries.
    let expected = derive_message_id(&author, dto.lamport, &parents, &dto.body);
    if expected != id {
        return Err(WireError::IdMismatch);
    }
    Ok(WireMessage {
        id,
        author,
        lamport: dto.lamport,
        parents,
        body: dto.body,
        ts_hint_ms: dto.ts_hint_ms,
    })
}

fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    let mut out = Vec::new();
    // CBOR encoding of these DTOs cannot fail on well-formed input.
    if ciborium::into_writer(value, &mut out).is_err() {
        out.clear();
    }
    out
}

/// Sign a channel signal into an envelope.
pub fn seal_signal(keys: &LocalKeys, signal: &ChannelSignal) -> SignedEnvelope {
    let payload = encode(&signal_dto(signal));
    let sig = keys.sign(&payload);
    SignedEnvelope {
        payload,
        author: keys.identity_id().as_bytes().to_vec(),
        sig: sig.to_vec(),
    }
}

/// Sign a DM into an envelope.
pub fn seal_dm(keys: &LocalKeys, message: &WireMessage) -> SignedEnvelope {
    let payload = encode(&DmPayloadDto::Chat(msg_dto(message)));
    let sig = keys.sign(&payload);
    SignedEnvelope {
        payload,
        author: keys.identity_id().as_bytes().to_vec(),
        sig: sig.to_vec(),
    }
}

/// Sign a file offer into an envelope.
pub fn seal_offer(keys: &LocalKeys, manifest: &FileManifest) -> SignedEnvelope {
    let payload = encode(&DmPayloadDto::Offer(manifest_dto(manifest)));
    let sig = keys.sign(&payload);
    SignedEnvelope {
        payload,
        author: keys.identity_id().as_bytes().to_vec(),
        sig: sig.to_vec(),
    }
}

fn check_signature(envelope: &SignedEnvelope) -> Result<(IdentityId, Verified), WireError> {
    if envelope.payload.len() > MAX_ENVELOPE_BYTES {
        return Err(WireError::Malformed("envelope too large".into()));
    }
    let author = IdentityId::from_bytes(to_array32(&envelope.author)?);
    let sig: [u8; 64] = envelope
        .sig
        .as_slice()
        .try_into()
        .map_err(|_| WireError::Malformed("expected 64 signature bytes".into()))?;
    let verified =
        verify_signature(&author, &envelope.payload, &sig).map_err(|_| WireError::BadSignature)?;
    Ok((author, verified))
}

/// Verify and decode a channel envelope. Membership/topic/presence ops must
/// be self-signed: the actor inside the payload must be the envelope author.
pub fn open_envelope(envelope: &SignedEnvelope) -> Result<(ChannelSignal, Verified), WireError> {
    let (author, verified) = check_signature(envelope)?;
    let dto: SignalDto = ciborium::from_reader(envelope.payload.as_slice())
        .map_err(|e| WireError::Malformed(e.to_string()))?;
    let signal = match dto {
        SignalDto::Msg(m) => {
            let message = parse_msg(m)?;
            if message.author != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::Message(message)
        }
        SignalDto::Joined { who, nickname } => {
            let who = IdentityId::from_bytes(to_array32(&who)?);
            if who != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::Joined { who, nickname }
        }
        SignalDto::Left { who } => {
            let who = IdentityId::from_bytes(to_array32(&who)?);
            if who != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::Left { who }
        }
        SignalDto::Topic { by, topic } => {
            let by = IdentityId::from_bytes(to_array32(&by)?);
            if by != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::TopicChanged { by, topic }
        }
        SignalDto::Role { by, target, op } => {
            let by = IdentityId::from_bytes(to_array32(&by)?);
            if by != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::RoleChanged {
                by,
                target: IdentityId::from_bytes(to_array32(&target)?),
                op,
            }
        }
        SignalDto::Presence { who, away } => {
            let who = IdentityId::from_bytes(to_array32(&who)?);
            if who != author {
                return Err(WireError::AuthorMismatch);
            }
            ChannelSignal::Presence { who, away }
        }
    };
    Ok((signal, verified))
}

/// Verify and decode a direct-protocol envelope (chat or file offer).
/// Returns the verified author so the caller knows who sent the offer.
pub fn open_dm(envelope: &SignedEnvelope) -> Result<(IdentityId, DmPayload, Verified), WireError> {
    let (author, verified) = check_signature(envelope)?;
    let dto: DmPayloadDto = ciborium::from_reader(envelope.payload.as_slice())
        .map_err(|e| WireError::Malformed(e.to_string()))?;
    let payload = match dto {
        DmPayloadDto::Chat(m) => {
            let message = parse_msg(m)?;
            if message.author != author {
                return Err(WireError::AuthorMismatch);
            }
            DmPayload::Chat(message)
        }
        DmPayloadDto::Offer(m) => DmPayload::Offer(parse_manifest(m)?),
    };
    Ok((author, payload, verified))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn wire_message(keys: &LocalKeys, body: &str) -> WireMessage {
        let author = keys.identity_id();
        let id = derive_message_id(&author, 1, &[], body);
        WireMessage {
            id,
            author,
            lamport: 1,
            parents: vec![],
            body: body.to_owned(),
            ts_hint_ms: 42,
        }
    }

    #[test]
    fn signal_roundtrip() {
        let keys = LocalKeys::ephemeral();
        let signal = ChannelSignal::Message(wire_message(&keys, "negi"));
        let envelope = seal_signal(&keys, &signal);
        let (opened, _verified) = open_envelope(&envelope).unwrap();
        assert_eq!(opened, signal);
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let keys = LocalKeys::ephemeral();
        let mut envelope = seal_signal(&keys, &ChannelSignal::Message(wire_message(&keys, "a")));
        envelope.payload[0] ^= 0xFF;
        assert!(matches!(
            open_envelope(&envelope),
            Err(WireError::BadSignature) | Err(WireError::Malformed(_))
        ));
    }

    #[test]
    fn forged_message_id_is_rejected() {
        let keys = LocalKeys::ephemeral();
        let mut message = wire_message(&keys, "honest body");
        message.id = MessageId::from_bytes([9; 32]); // lie about the id
        let envelope = seal_dm(&keys, &message);
        assert!(matches!(open_dm(&envelope), Err(WireError::IdMismatch)));
    }

    #[test]
    fn join_op_impersonation_is_rejected() {
        let keys = LocalKeys::ephemeral();
        let other = LocalKeys::ephemeral();
        let signal = ChannelSignal::Joined {
            who: other.identity_id(), // claim someone else joined
            nickname: None,
        };
        let envelope = seal_signal(&keys, &signal);
        assert!(matches!(
            open_envelope(&envelope),
            Err(WireError::AuthorMismatch)
        ));
    }

    #[test]
    fn dm_from_wrong_author_is_rejected() {
        let keys = LocalKeys::ephemeral();
        let other = LocalKeys::ephemeral();
        let mut message = wire_message(&other, "hi");
        // re-derive id so the content check passes but authorship doesn't
        message.id = derive_message_id(&message.author, 1, &[], "hi");
        let envelope = seal_dm(&keys, &message);
        assert!(matches!(open_dm(&envelope), Err(WireError::AuthorMismatch)));
    }
}
