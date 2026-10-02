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
    // MD5 of the part's bytes, hex-encoded. Always present: `UploadPartCopy`
    // points at the source's shared blobs but still reads the copied window
    // back once to hash it, so every part carries a real digest.
    pub(crate) hash: String,
    pub(crate) metadata_items: Vec<MetadataItem>,
}
