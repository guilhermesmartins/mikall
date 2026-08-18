//! File-transfer use cases: offer, accept, verified chunk fetching, and
//! reassembly. Chunks are content-addressed (BLAKE3) and every chunk is
//! verified against the signed manifest before it counts; a corrupt chunk
//! is re-fetched, never trusted.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::shared::IdentityId;
use mikall_domain::transfer::{
    Direction, FileManifest, Transfer, TransferError, TransferId, TransferPhase,
};

use crate::events::{AppEvent, EventBus};
use crate::ports::{BlobStore, ChunkHasher, FileTransport, IdGen, StoreError, TransportError};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransferServiceError {
    #[error("unknown transfer")]
    UnknownTransfer,
    #[error(transparent)]
    Transfer(#[from] TransferError),
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("transfer has no remote peer to fetch from")]
    NoPeer,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferProgress {
    pub id: TransferId,
    pub phase: TransferPhase,
    pub have_chunks: u32,
    pub total_chunks: u32,
}

#[derive(Clone)]
pub struct TransferService {
    idgen: Arc<dyn IdGen>,
    bus: EventBus,
    transport: Arc<dyn FileTransport>,
    blobs: Arc<dyn BlobStore>,
    hasher: Arc<dyn ChunkHasher>,
    save_dir: PathBuf,
    transfers: Arc<RwLock<BTreeMap<TransferId, Transfer>>>,
}

impl std::fmt::Debug for TransferService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferService").finish_non_exhaustive()
    }
}

impl TransferService {
    pub fn new(
        idgen: Arc<dyn IdGen>,
        bus: EventBus,
        transport: Arc<dyn FileTransport>,
        blobs: Arc<dyn BlobStore>,
        hasher: Arc<dyn ChunkHasher>,
        save_dir: PathBuf,
    ) -> Self {
        TransferService {
            idgen,
            bus,
            transport,
            blobs,
            hasher,
            save_dir,
            transfers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Import a local file into the blob store and offer it to a peer.
    pub async fn offer_path(
        &self,
        to: IdentityId,
        path: &std::path::Path,
    ) -> Result<TransferId, TransferServiceError> {
        let manifest = self.blobs.import(path).await?;
        self.offer_file(to, manifest).await
    }

    /// Offer a file whose chunks are already in the blob store.
    pub async fn offer_file(
        &self,
        to: IdentityId,
        manifest: FileManifest,
    ) -> Result<TransferId, TransferServiceError> {
        let id = self.idgen.transfer_id();
        let (transfer, event) = Transfer::offered(id, manifest.clone(), Direction::Outbound { to });
        self.transfers.write().await.insert(id, transfer);
        self.transport.send_offer(to, manifest).await?;
        self.bus.publish_domain(event);
        Ok(id)
    }

    /// An inbound offer arrived from the network (via the router).
    pub async fn offered_to_us(&self, from: IdentityId, manifest: FileManifest) -> TransferId {
        let id = self.idgen.transfer_id();
        let (transfer, event) = Transfer::offered(id, manifest, Direction::Inbound { from });
        self.transfers.write().await.insert(id, transfer);
        self.bus.publish_domain(event);
        id
    }

    /// Accept an inbound offer and start fetching + verifying chunks in the
    /// background. Progress and completion surface as domain events.
    pub async fn accept(&self, id: TransferId) -> Result<(), TransferServiceError> {
        let (event, from) = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            let from = match transfer.direction() {
                Direction::Inbound { from } => Some(from),
                Direction::Outbound { .. } => None,
            };
            (transfer.accept()?, from)
        };
        self.bus.publish_domain(event);
        if let Some(from) = from {
            let service = self.clone();
            tokio::spawn(async move {
                if let Err(err) = service.run_fetch(id, from).await {
                    let _ = service.fail(id, &err.to_string()).await;
                }
            });
        }
        Ok(())
    }

    /// Sequential verified fetch loop (windowing is a later optimization).
    async fn run_fetch(
        &self,
        id: TransferId,
        from: IdentityId,
    ) -> Result<(), TransferServiceError> {
        loop {
            let (manifest, next) = {
                let transfers = self.transfers.read().await;
                let transfer = transfers
                    .get(&id)
                    .ok_or(TransferServiceError::UnknownTransfer)?;
                match transfer.phase() {
                    TransferPhase::Complete
                    | TransferPhase::Rejected
                    | TransferPhase::Failed { .. } => return Ok(()),
                    TransferPhase::Offered
                    | TransferPhase::Accepted
                    | TransferPhase::Transferring => {}
                }
                let next = transfer.bitmap().missing().next();
                (transfer.manifest().clone(), next)
            };
            let Some(index) = next else {
                return Ok(()); // nothing missing — completion already handled
            };
            let bytes = self
                .transport
                .fetch_chunk(from, manifest.root(), index)
                .await?;
            let matches = manifest.chunk_hash(index) == Some(self.hasher.hash_chunk(&bytes));
            if matches {
                self.blobs.put_chunk(manifest.root(), index, &bytes).await?;
            }
            match self.chunk_verified(id, index, matches).await {
                Ok(()) => {}
                Err(TransferServiceError::Transfer(TransferError::ChunkHashMismatch {
                    ..
                })) => continue, // corrupt: re-fetch the same chunk
                Err(err) => return Err(err),
            }
            let complete = {
                let transfers = self.transfers.read().await;
                transfers
                    .get(&id)
                    .is_some_and(|t| matches!(t.phase(), TransferPhase::Complete))
            };
            if complete {
                let dest = self.save_dir.join(manifest.name().as_str());
                self.blobs.assemble(&manifest, &dest).await?;
                self.bus.publish(AppEvent::TransferSaved {
                    transfer: id,
                    path: dest.display().to_string(),
                });
                return Ok(());
            }
        }
    }

    pub async fn reject(&self, id: TransferId) -> Result<(), TransferServiceError> {
        let event = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            transfer.reject()?
        };
        self.bus.publish_domain(event);
        Ok(())
    }

    /// Record the verification result for one chunk (also used directly by
    /// the BDD suite to drive the aggregate).
    pub async fn chunk_verified(
        &self,
        id: TransferId,
        index: u32,
        hash_matches: bool,
    ) -> Result<(), TransferServiceError> {
        let result = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            transfer.chunk_verified(index, hash_matches)
        };
        match result {
            Ok(events) => {
                for event in events {
                    self.bus.publish_domain(event);
                }
                Ok(())
            }
            Err(TransferError::ChunkHashMismatch { index }) => {
                // Recorded but recoverable: the fetch loop re-requests.
                Err(TransferServiceError::Transfer(
                    TransferError::ChunkHashMismatch { index },
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn fail(&self, id: TransferId, reason: &str) -> Result<(), TransferServiceError> {
        let event = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            transfer.fail(reason)?
        };
        self.bus.publish_domain(event);
        Ok(())
    }

    pub async fn progress(&self, id: TransferId) -> Result<TransferProgress, TransferServiceError> {
        let transfers = self.transfers.read().await;
        let transfer = transfers
            .get(&id)
            .ok_or(TransferServiceError::UnknownTransfer)?;
        Ok(TransferProgress {
            id,
            phase: transfer.phase().clone(),
            have_chunks: transfer.bitmap().have_count(),
            total_chunks: transfer.bitmap().len(),
        })
    }

    pub async fn list(&self) -> Vec<TransferProgress> {
        self.transfers
            .read()
            .await
            .values()
            .map(|t| TransferProgress {
                id: t.id(),
                phase: t.phase().clone(),
                have_chunks: t.bitmap().have_count(),
                total_chunks: t.bitmap().len(),
            })
            .collect()
    }

    /// Chunk indexes still missing — what the fetcher requests next.
    pub async fn missing_chunks(&self, id: TransferId) -> Result<Vec<u32>, TransferServiceError> {
        let transfers = self.transfers.read().await;
        let transfer = transfers
            .get(&id)
            .ok_or(TransferServiceError::UnknownTransfer)?;
        Ok(transfer.bitmap().missing().collect())
    }
}
