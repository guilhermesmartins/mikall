//! File-transfer use cases. The chunk network protocol arrives in M4; the
//! aggregate — offer/accept/reject lifecycle, verified-chunk bitmap,
//! completion — is fully driven here already.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::sync::RwLock;

use mikall_domain::shared::IdentityId;
use mikall_domain::transfer::{
    Direction, FileManifest, Transfer, TransferError, TransferId, TransferPhase,
};

use crate::events::EventBus;
use crate::ports::IdGen;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransferServiceError {
    #[error("unknown transfer")]
    UnknownTransfer,
    #[error(transparent)]
    Transfer(#[from] TransferError),
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
    transfers: Arc<RwLock<BTreeMap<TransferId, Transfer>>>,
}

impl std::fmt::Debug for TransferService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransferService").finish_non_exhaustive()
    }
}

impl TransferService {
    pub fn new(idgen: Arc<dyn IdGen>, bus: EventBus) -> Self {
        TransferService {
            idgen,
            bus,
            transfers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    /// Offer a file to a peer (the DCC SEND / drag-and-drop use case).
    pub async fn offer_file(&self, to: IdentityId, manifest: FileManifest) -> TransferId {
        let id = self.idgen.transfer_id();
        let (transfer, event) = Transfer::offered(id, manifest, Direction::Outbound { to });
        self.transfers.write().await.insert(id, transfer);
        self.bus.publish_domain(event);
        id
    }

    /// An inbound offer arrived from the network.
    pub async fn offered_to_us(&self, from: IdentityId, manifest: FileManifest) -> TransferId {
        let id = self.idgen.transfer_id();
        let (transfer, event) = Transfer::offered(id, manifest, Direction::Inbound { from });
        self.transfers.write().await.insert(id, transfer);
        self.bus.publish_domain(event);
        id
    }

    pub async fn accept(&self, id: TransferId) -> Result<(), TransferServiceError> {
        let event = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            transfer.accept()?
        };
        self.bus.publish_domain(event);
        Ok(())
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

    /// A chunk arrived and its BLAKE3 hash was compared against the manifest
    /// by the blob adapter.
    pub async fn chunk_verified(
        &self,
        id: TransferId,
        index: u32,
        hash_matches: bool,
    ) -> Result<(), TransferServiceError> {
        let events = {
            let mut transfers = self.transfers.write().await;
            let transfer = transfers
                .get_mut(&id)
                .ok_or(TransferServiceError::UnknownTransfer)?;
            transfer.chunk_verified(index, hash_matches)?
        };
        for event in events {
            self.bus.publish_domain(event);
        }
        Ok(())
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

    /// Chunk indexes still missing — what the fetcher should request next.
    pub async fn missing_chunks(&self, id: TransferId) -> Result<Vec<u32>, TransferServiceError> {
        let transfers = self.transfers.read().await;
        let transfer = transfers
            .get(&id)
            .ok_or(TransferServiceError::UnknownTransfer)?;
        Ok(transfer.bitmap().missing().collect())
    }
}
