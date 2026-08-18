//! Crypto adapter: Ed25519 identity custody, envelope signing and
//! verification, BLAKE3 content-derived ids, and the system clock.
//!
//! This crate is the *only* place where private key material lives, and the
//! only production code path that mints a
//! [`mikall_domain::messaging::Verified`] attestation — always immediately
//! after an actual signature check.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use ed25519_dalek::{Signature as DalekSignature, Signer, SigningKey, Verifier, VerifyingKey};
use zeroize::Zeroizing;

use mikall_app::ports::{Clock, IdGen, KeyStore, KeyStoreError};
use mikall_domain::calls::CallId;
use mikall_domain::messaging::{ChannelId, ChannelName, MessageId, Verified};
use mikall_domain::shared::{Fingerprint, IdentityId};
use mikall_domain::transfer::TransferId;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("i/o error handling key file: {0}")]
    Io(#[from] std::io::Error),
    #[error("key file is corrupt (expected 32 secret bytes)")]
    CorruptKeyFile,
    #[error("invalid public key bytes")]
    BadPublicKey,
    #[error("signature verification failed")]
    BadSignature,
}

/// Fingerprint = BLAKE3 of the public key — one rule, everywhere.
pub fn fingerprint_of(id: &IdentityId) -> Fingerprint {
    Fingerprint::from_bytes(*blake3::hash(id.as_bytes()).as_bytes())
}

/// The local identity: an Ed25519 keypair persisted as 32 secret bytes with
/// owner-only permissions. Loading a missing file generates a new identity.
pub struct LocalKeys {
    signing: SigningKey,
    path: Option<PathBuf>,
}

impl std::fmt::Debug for LocalKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalKeys")
            .field("id", &self.identity_id())
            .finish_non_exhaustive()
    }
}

impl LocalKeys {
    /// Load the key file, or generate + persist a fresh identity.
    pub fn load_or_generate(path: &Path) -> Result<Self, CryptoError> {
        if path.exists() {
            let bytes = Zeroizing::new(std::fs::read(path)?);
            let secret: [u8; 32] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| CryptoError::CorruptKeyFile)?;
            Ok(LocalKeys {
                signing: SigningKey::from_bytes(&secret),
                path: Some(path.to_owned()),
            })
        } else {
            let keys = LocalKeys {
                signing: SigningKey::generate(&mut rand::rngs::OsRng),
                path: Some(path.to_owned()),
            };
            keys.persist()?;
            Ok(keys)
        }
    }

    /// An in-memory identity (tests, throwaway nodes).
    pub fn ephemeral() -> Self {
        LocalKeys {
            signing: SigningKey::generate(&mut rand::rngs::OsRng),
            path: None,
        }
    }

    fn persist(&self) -> Result<(), CryptoError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, self.signing.to_bytes())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    pub fn identity_id(&self) -> IdentityId {
        IdentityId::from_bytes(self.signing.verifying_key().to_bytes())
    }

    pub fn fingerprint(&self) -> Fingerprint {
        fingerprint_of(&self.identity_id())
    }

    /// The raw secret, for deriving the libp2p transport keypair from the
    /// same identity (so `PeerId` is a pure function of `IdentityId`).
    pub fn secret_bytes(&self) -> Zeroizing<[u8; 32]> {
        Zeroizing::new(self.signing.to_bytes())
    }

    pub fn sign(&self, message: &[u8]) -> [u8; 64] {
        self.signing.sign(message).to_bytes()
    }
}

/// Verify `sig` by `author` over `message`; on success mint the domain's
/// verification attestation. This is the trust boundary.
pub fn verify_signature(
    author: &IdentityId,
    message: &[u8],
    sig: &[u8; 64],
) -> Result<Verified, CryptoError> {
    let key = VerifyingKey::from_bytes(author.as_bytes()).map_err(|_| CryptoError::BadPublicKey)?;
    key.verify(message, &DalekSignature::from_bytes(sig))
        .map_err(|_| CryptoError::BadSignature)?;
    Ok(Verified::attest_signature_checked())
}

/// The canonical content-derived message id. Sender and every receiver
/// compute the same value; a mismatch is a forgery and the envelope is
/// dropped at the network boundary.
pub fn derive_message_id(
    author: &IdentityId,
    lamport: u64,
    parents: &[MessageId],
    body: &str,
) -> MessageId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mikall:msg:v1");
    hasher.update(author.as_bytes());
    hasher.update(&lamport.to_le_bytes());
    hasher.update(&(parents.len() as u64).to_le_bytes());
    for parent in parents {
        hasher.update(parent.as_bytes());
    }
    hasher.update(body.as_bytes());
    MessageId::from_bytes(*hasher.finalize().as_bytes())
}

/// Deterministic public-channel id: `JOIN #stage` converges everywhere.
pub fn derive_channel_id(name: &ChannelName) -> ChannelId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"mikall:chan:v1");
    hasher.update(name.as_str().as_bytes());
    ChannelId::from_bytes(*hasher.finalize().as_bytes())
}

/// Production `IdGen`: BLAKE3 for content-derived ids, UUIDv7 for entity ids.
#[derive(Debug, Default)]
pub struct CryptoIdGen;

impl IdGen for CryptoIdGen {
    fn message_id(
        &self,
        author: &IdentityId,
        lamport: u64,
        parents: &[MessageId],
        body: &str,
    ) -> MessageId {
        derive_message_id(author, lamport, parents, body)
    }

    fn channel_id(&self, name: &ChannelName) -> ChannelId {
        derive_channel_id(name)
    }

    fn call_id(&self) -> CallId {
        CallId::from_bytes(*uuid::Uuid::now_v7().as_bytes())
    }

    fn transfer_id(&self) -> TransferId {
        TransferId::from_bytes(*uuid::Uuid::now_v7().as_bytes())
    }
}

/// Production `KeyStore` over [`LocalKeys`].
#[derive(Debug)]
pub struct FileKeyStore {
    keys: Arc<LocalKeys>,
}

impl FileKeyStore {
    pub fn new(keys: Arc<LocalKeys>) -> Self {
        FileKeyStore { keys }
    }
}

#[async_trait]
impl KeyStore for FileKeyStore {
    async fn local_identity(&self) -> Result<(IdentityId, Fingerprint), KeyStoreError> {
        Ok((self.keys.identity_id(), self.keys.fingerprint()))
    }

    fn fingerprint_of(&self, id: &IdentityId) -> Fingerprint {
        fingerprint_of(id)
    }
}

/// Wall-clock milliseconds.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn sign_verify_roundtrip() {
        let keys = LocalKeys::ephemeral();
        let sig = keys.sign(b"negi");
        assert!(verify_signature(&keys.identity_id(), b"negi", &sig).is_ok());
        assert!(verify_signature(&keys.identity_id(), b"leek", &sig).is_err());
    }

    #[test]
    fn tampered_author_fails() {
        let keys = LocalKeys::ephemeral();
        let other = LocalKeys::ephemeral();
        let sig = keys.sign(b"negi");
        assert!(verify_signature(&other.identity_id(), b"negi", &sig).is_err());
    }

    #[test]
    fn message_id_is_content_derived_and_stable() {
        let author = IdentityId::from_bytes([1; 32]);
        let a = derive_message_id(&author, 1, &[], "hi");
        let b = derive_message_id(&author, 1, &[], "hi");
        let c = derive_message_id(&author, 2, &[], "hi");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn key_file_roundtrip() {
        let dir = std::env::temp_dir().join(format!("mikall-key-test-{}", std::process::id()));
        let path = dir.join("identity.key");
        let first = LocalKeys::load_or_generate(&path).unwrap();
        let second = LocalKeys::load_or_generate(&path).unwrap();
        assert_eq!(first.identity_id(), second.identity_id());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
