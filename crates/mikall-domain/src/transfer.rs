//! FileTransfer bounded context: content-addressed, chunked, resumable.

use core::fmt;

use crate::shared::IdentityId;

/// 256 KiB chunks.
pub const CHUNK_SIZE: u64 = 256 * 1024;
/// 2 GiB ceiling.
pub const MAX_FILE_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// A file name safe to write to disk: 1..=255 bytes, no path separators, no
/// NUL/control characters, no leading dot (no hidden files, no `..`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileName(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FileNameError {
    #[error("file name must not be empty")]
    Empty,
    #[error("file name exceeds 255 bytes")]
    TooLong,
    #[error("file name must not contain path separators")]
    PathSeparator,
    #[error("file name must not start with a dot")]
    LeadingDot,
    #[error("file name contains a control character")]
    ControlCharacter,
}

impl FileName {
    pub fn parse(raw: &str) -> Result<Self, FileNameError> {
        if raw.is_empty() {
            return Err(FileNameError::Empty);
        }
        if raw.len() > 255 {
            return Err(FileNameError::TooLong);
        }
        if raw.contains('/') || raw.contains('\\') {
            return Err(FileNameError::PathSeparator);
        }
        if raw.starts_with('.') {
            return Err(FileNameError::LeadingDot);
        }
        if raw.chars().any(char::is_control) {
            return Err(FileNameError::ControlCharacter);
        }
        Ok(FileName(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// BLAKE3 digest of a chunk or of the whole blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobHash([u8; 32]);

impl BlobHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        BlobHash(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobHash(")?;
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

/// Signed description of a file on offer. Chunk count is derived from the
/// size and must match the hash list — a manifest with a mismatched chunk
/// list cannot be constructed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileManifest {
    name: FileName,
    size: u64,
    root: BlobHash,
    chunk_hashes: Vec<BlobHash>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("file size must be 1..={MAX_FILE_SIZE} bytes")]
    BadSize,
    #[error("chunk hash list does not match file size (expected {expected} chunks)")]
    ChunkCountMismatch { expected: u64 },
}

impl FileManifest {
    pub fn new(
        name: FileName,
        size: u64,
        root: BlobHash,
        chunk_hashes: Vec<BlobHash>,
    ) -> Result<Self, ManifestError> {
        if size == 0 || size > MAX_FILE_SIZE {
            return Err(ManifestError::BadSize);
        }
        let expected = size.div_ceil(CHUNK_SIZE);
        if chunk_hashes.len() as u64 != expected {
            return Err(ManifestError::ChunkCountMismatch { expected });
        }
        Ok(FileManifest {
            name,
            size,
            root,
            chunk_hashes,
        })
    }

    pub fn name(&self) -> &FileName {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn root(&self) -> BlobHash {
        self.root
    }

    pub fn chunk_count(&self) -> u32 {
        self.chunk_hashes.len() as u32
    }

    pub fn chunk_hash(&self, index: u32) -> Option<BlobHash> {
        self.chunk_hashes.get(index as usize).copied()
    }
}

/// Which chunks we hold. Cannot address beyond the manifest's chunk count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkBitmap {
    bits: Vec<u64>,
    len: u32,
    set_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("chunk index {index} out of range (chunk count {len})")]
pub struct ChunkOutOfRange {
    pub index: u32,
    pub len: u32,
}

impl ChunkBitmap {
    pub fn empty(len: u32) -> Self {
        ChunkBitmap {
            bits: vec![0; (len as usize).div_ceil(64)],
            len,
            set_count: 0,
        }
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn have(&self, index: u32) -> bool {
        if index >= self.len {
            return false;
        }
        self.bits[(index / 64) as usize] & (1u64 << (index % 64)) != 0
    }

    /// Returns `true` if the bit was newly set.
    pub fn set(&mut self, index: u32) -> Result<bool, ChunkOutOfRange> {
        if index >= self.len {
            return Err(ChunkOutOfRange {
                index,
                len: self.len,
            });
        }
        let slot = &mut self.bits[(index / 64) as usize];
        let mask = 1u64 << (index % 64);
        if *slot & mask != 0 {
            return Ok(false);
        }
        *slot |= mask;
        self.set_count += 1;
        Ok(true)
    }

    pub fn complete(&self) -> bool {
        self.set_count == self.len
    }

    pub fn have_count(&self) -> u32 {
        self.set_count
    }

    pub fn missing(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.len).filter(|i| !self.have(*i))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransferId([u8; 16]);

impl TransferId {
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        TransferId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TransferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TransferId(")?;
        for b in &self.0[..6] {
            write!(f, "{b:02x}")?;
        }
        write!(f, ")")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound { from: IdentityId },
    Outbound { to: IdentityId },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferPhase {
    Offered,
    Accepted,
    Transferring,
    Complete,
    Rejected,
    Failed { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransferError {
    #[error("invalid transfer transition: {action} while {state}")]
    InvalidTransition {
        action: &'static str,
        state: &'static str,
    },
    #[error(transparent)]
    OutOfRange(#[from] ChunkOutOfRange),
    #[error("chunk {index} hash mismatch")]
    ChunkHashMismatch { index: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferEvent {
    FileOffered {
        transfer: TransferId,
        name: FileName,
        size: u64,
    },
    TransferAccepted {
        transfer: TransferId,
    },
    TransferRejected {
        transfer: TransferId,
    },
    ChunkVerified {
        transfer: TransferId,
        index: u32,
        have: u32,
        total: u32,
    },
    TransferCompleted {
        transfer: TransferId,
        root: BlobHash,
    },
    TransferFailed {
        transfer: TransferId,
        reason: String,
    },
}

/// The `Transfer` aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transfer {
    id: TransferId,
    manifest: FileManifest,
    direction: Direction,
    phase: TransferPhase,
    bitmap: ChunkBitmap,
}

impl Transfer {
    pub fn offered(
        id: TransferId,
        manifest: FileManifest,
        direction: Direction,
    ) -> (Self, TransferEvent) {
        let bitmap = ChunkBitmap::empty(manifest.chunk_count());
        let event = TransferEvent::FileOffered {
            transfer: id,
            name: manifest.name().clone(),
            size: manifest.size(),
        };
        (
            Transfer {
                id,
                manifest,
                direction,
                phase: TransferPhase::Offered,
                bitmap,
            },
            event,
        )
    }

    pub fn id(&self) -> TransferId {
        self.id
    }

    pub fn manifest(&self) -> &FileManifest {
        &self.manifest
    }

    pub fn direction(&self) -> Direction {
        self.direction
    }

    pub fn phase(&self) -> &TransferPhase {
        &self.phase
    }

    pub fn bitmap(&self) -> &ChunkBitmap {
        &self.bitmap
    }

    fn state_name(&self) -> &'static str {
        match self.phase {
            TransferPhase::Offered => "offered",
            TransferPhase::Accepted => "accepted",
            TransferPhase::Transferring => "transferring",
            TransferPhase::Complete => "complete",
            TransferPhase::Rejected => "rejected",
            TransferPhase::Failed { .. } => "failed",
        }
    }

    pub fn accept(&mut self) -> Result<TransferEvent, TransferError> {
        match self.phase {
            TransferPhase::Offered => {
                self.phase = TransferPhase::Accepted;
                Ok(TransferEvent::TransferAccepted { transfer: self.id })
            }
            TransferPhase::Accepted
            | TransferPhase::Transferring
            | TransferPhase::Complete
            | TransferPhase::Rejected
            | TransferPhase::Failed { .. } => Err(TransferError::InvalidTransition {
                action: "accept",
                state: self.state_name(),
            }),
        }
    }

    pub fn reject(&mut self) -> Result<TransferEvent, TransferError> {
        match self.phase {
            TransferPhase::Offered => {
                self.phase = TransferPhase::Rejected;
                Ok(TransferEvent::TransferRejected { transfer: self.id })
            }
            TransferPhase::Accepted
            | TransferPhase::Transferring
            | TransferPhase::Complete
            | TransferPhase::Rejected
            | TransferPhase::Failed { .. } => Err(TransferError::InvalidTransition {
                action: "reject",
                state: self.state_name(),
            }),
        }
    }

    /// Record a chunk whose hash the adapter verified against the manifest.
    /// `hash_matches` is the result of that comparison; a mismatch fails the
    /// chunk (the caller re-fetches), never poisons the bitmap.
    pub fn chunk_verified(
        &mut self,
        index: u32,
        hash_matches: bool,
    ) -> Result<Vec<TransferEvent>, TransferError> {
        match self.phase {
            TransferPhase::Accepted | TransferPhase::Transferring => {}
            TransferPhase::Offered
            | TransferPhase::Complete
            | TransferPhase::Rejected
            | TransferPhase::Failed { .. } => {
                return Err(TransferError::InvalidTransition {
                    action: "chunk_verified",
                    state: self.state_name(),
                })
            }
        }
        if !hash_matches {
            return Err(TransferError::ChunkHashMismatch { index });
        }
        self.phase = TransferPhase::Transferring;
        let newly = self.bitmap.set(index)?;
        let mut events = Vec::new();
        if newly {
            events.push(TransferEvent::ChunkVerified {
                transfer: self.id,
                index,
                have: self.bitmap.have_count(),
                total: self.bitmap.len(),
            });
        }
        if self.bitmap.complete() {
            self.phase = TransferPhase::Complete;
            events.push(TransferEvent::TransferCompleted {
                transfer: self.id,
                root: self.manifest.root(),
            });
        }
        Ok(events)
    }

    pub fn fail(&mut self, reason: &str) -> Result<TransferEvent, TransferError> {
        match self.phase {
            TransferPhase::Offered | TransferPhase::Accepted | TransferPhase::Transferring => {
                self.phase = TransferPhase::Failed {
                    reason: reason.to_owned(),
                };
                Ok(TransferEvent::TransferFailed {
                    transfer: self.id,
                    reason: reason.to_owned(),
                })
            }
            TransferPhase::Complete | TransferPhase::Rejected | TransferPhase::Failed { .. } => {
                Err(TransferError::InvalidTransition {
                    action: "fail",
                    state: self.state_name(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn manifest(size: u64) -> FileManifest {
        let chunks = size.div_ceil(CHUNK_SIZE);
        FileManifest::new(
            FileName::parse("mix.flac").unwrap(),
            size,
            BlobHash::from_bytes([1; 32]),
            (0..chunks)
                .map(|i| BlobHash::from_bytes([i as u8; 32]))
                .collect(),
        )
        .unwrap()
    }

    fn inbound(size: u64) -> Transfer {
        Transfer::offered(
            TransferId::from_bytes([1; 16]),
            manifest(size),
            Direction::Inbound {
                from: IdentityId::from_bytes([2; 32]),
            },
        )
        .0
    }

    #[test]
    fn file_name_rejects_traversal_and_hidden() {
        assert!(FileName::parse("../etc/passwd").is_err());
        assert!(FileName::parse("a/b").is_err());
        assert!(FileName::parse("a\\b").is_err());
        assert!(FileName::parse(".bashrc").is_err());
        assert!(FileName::parse("").is_err());
        assert!(FileName::parse("song (final) v2.flac").is_ok());
    }

    #[test]
    fn manifest_chunk_count_must_match_size() {
        let err = FileManifest::new(
            FileName::parse("x.bin").unwrap(),
            CHUNK_SIZE + 1,
            BlobHash::from_bytes([0; 32]),
            vec![BlobHash::from_bytes([0; 32])], // needs 2
        );
        assert_eq!(err, Err(ManifestError::ChunkCountMismatch { expected: 2 }));
        assert_eq!(
            FileManifest::new(
                FileName::parse("x.bin").unwrap(),
                0,
                BlobHash::from_bytes([0; 32]),
                vec![],
            ),
            Err(ManifestError::BadSize)
        );
    }

    #[test]
    fn transfer_completes_when_all_chunks_verify() {
        let mut t = inbound(CHUNK_SIZE * 2);
        t.accept().unwrap();
        let events = t.chunk_verified(0, true).unwrap();
        assert_eq!(events.len(), 1);
        let events = t.chunk_verified(1, true).unwrap();
        assert!(matches!(
            events.last(),
            Some(TransferEvent::TransferCompleted { .. })
        ));
        assert_eq!(t.phase(), &TransferPhase::Complete);
    }

    #[test]
    fn corrupt_chunk_fails_without_setting_bit() {
        let mut t = inbound(CHUNK_SIZE);
        t.accept().unwrap();
        assert_eq!(
            t.chunk_verified(0, false),
            Err(TransferError::ChunkHashMismatch { index: 0 })
        );
        assert!(!t.bitmap().have(0));
        // Re-fetch succeeds.
        t.chunk_verified(0, true).unwrap();
        assert_eq!(t.phase(), &TransferPhase::Complete);
    }

    #[test]
    fn out_of_range_chunk_is_unrepresentable() {
        let mut t = inbound(CHUNK_SIZE);
        t.accept().unwrap();
        assert!(matches!(
            t.chunk_verified(5, true),
            Err(TransferError::OutOfRange(_))
        ));
    }

    #[test]
    fn rejected_transfer_stays_rejected() {
        let mut t = inbound(CHUNK_SIZE);
        t.reject().unwrap();
        assert!(t.accept().is_err());
        assert!(t.chunk_verified(0, true).is_err());
    }

    #[test]
    fn duplicate_chunk_is_silent() {
        let mut t = inbound(CHUNK_SIZE * 2);
        t.accept().unwrap();
        t.chunk_verified(0, true).unwrap();
        let events = t.chunk_verified(0, true).unwrap();
        assert!(events.is_empty());
    }
}
