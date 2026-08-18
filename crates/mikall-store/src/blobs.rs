//! Content-addressed chunk storage on the filesystem:
//! `blobs/<root-hex>/<index>` holds one verified chunk. Any blob present
//! here can be served to peers.

use std::path::{Path, PathBuf};

use async_trait::async_trait;

use mikall_app::ports::{BlobStore, StoreError};
use mikall_domain::transfer::{BlobHash, FileManifest, FileName, CHUNK_SIZE, MAX_FILE_SIZE};

fn map_err<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Other(e.to_string())
}

fn hex(hash: &BlobHash) -> String {
    hash.as_bytes().iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Debug)]
pub struct FsBlobStore {
    dir: PathBuf,
}

impl FsBlobStore {
    pub fn new(dir: PathBuf) -> Result<Self, StoreError> {
        std::fs::create_dir_all(&dir).map_err(map_err)?;
        Ok(FsBlobStore { dir })
    }

    fn chunk_path(&self, root: &BlobHash, index: u32) -> PathBuf {
        self.dir.join(hex(root)).join(index.to_string())
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn put_chunk(&self, root: BlobHash, index: u32, bytes: &[u8]) -> Result<(), StoreError> {
        let path = self.chunk_path(&root, index);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(map_err)?;
        }
        std::fs::write(path, bytes).map_err(map_err)
    }

    async fn get_chunk(&self, root: BlobHash, index: u32) -> Result<Option<Vec<u8>>, StoreError> {
        match std::fs::read(self.chunk_path(&root, index)) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(map_err(e)),
        }
    }

    async fn import(&self, path: &Path) -> Result<FileManifest, StoreError> {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| StoreError::Other("path has no usable file name".into()))?;
        let name = FileName::parse(name).map_err(map_err)?;
        let bytes = std::fs::read(path).map_err(map_err)?;
        if bytes.is_empty() || bytes.len() as u64 > MAX_FILE_SIZE {
            return Err(StoreError::Other(format!(
                "file size {} outside 1..={MAX_FILE_SIZE}",
                bytes.len()
            )));
        }
        let chunks: Vec<&[u8]> = bytes.chunks(CHUNK_SIZE as usize).collect();
        let chunk_hashes: Vec<BlobHash> = chunks
            .iter()
            .map(|c| BlobHash::from_bytes(*blake3::hash(c).as_bytes()))
            .collect();
        let root = BlobHash::from_bytes(*blake3::hash(&bytes).as_bytes());
        let manifest =
            FileManifest::new(name, bytes.len() as u64, root, chunk_hashes).map_err(map_err)?;
        for (index, chunk) in chunks.iter().enumerate() {
            self.put_chunk(root, index as u32, chunk).await?;
        }
        Ok(manifest)
    }

    async fn assemble(&self, manifest: &FileManifest, dest: &Path) -> Result<(), StoreError> {
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent).map_err(map_err)?;
        }
        let mut out = Vec::with_capacity(manifest.size() as usize);
        for index in 0..manifest.chunk_count() {
            let chunk = self
                .get_chunk(manifest.root(), index)
                .await?
                .ok_or_else(|| StoreError::Other(format!("missing chunk {index}")))?;
            out.extend_from_slice(&chunk);
        }
        if out.len() as u64 != manifest.size() {
            return Err(StoreError::Other("assembled size mismatch".into()));
        }
        // Verify the whole blob against the manifest root before writing.
        if BlobHash::from_bytes(*blake3::hash(&out).as_bytes()) != manifest.root() {
            return Err(StoreError::Other("assembled root hash mismatch".into()));
        }
        std::fs::write(dest, out).map_err(map_err)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn import_serve_assemble_roundtrip() {
        let base = std::env::temp_dir().join(format!("mikall-blob-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let store = FsBlobStore::new(base.join("blobs")).unwrap();

        let src = base.join("song.bin");
        let data: Vec<u8> = (0..(CHUNK_SIZE + 1234)).map(|i| (i % 251) as u8).collect();
        std::fs::write(&src, &data).unwrap();

        let manifest = store.import(&src).await.unwrap();
        assert_eq!(manifest.chunk_count(), 2);
        assert!(store.get_chunk(manifest.root(), 0).await.unwrap().is_some());
        assert!(store.get_chunk(manifest.root(), 5).await.unwrap().is_none());

        let dest = base.join("out.bin");
        store.assemble(&manifest, &dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), data);

        std::fs::remove_dir_all(&base).unwrap();
    }
}
