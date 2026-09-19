use serde::{Deserialize, Serialize};

/// Ordered blob-item list describing an object's content.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub(crate) item: Vec<MetadataItem>,
}

/// One slice of a backend blob.
///
/// `offset` is the start of this item's data within the backend blob.
/// Zero for objects that own whole blobs; non-zero when an item is a
/// shared slice of a larger blob (e.g. produced by a ranged upload
/// part copy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetadataItem {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

/// One buffered multipart part: digest plus backing blob slices.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MultipartUploadPart {
    // Digest of the part's bytes; empty when unknown (parts created by
    // UploadPartCopy share existing blobs and skip re-reading them).
    pub(crate) hash: String,
    pub(crate) metadata_items: Vec<MetadataItem>,
}
