//! Codec between the S3 user-metadata map and its stored JSON document.

pub(crate) fn metadata_to_json(metadata: Option<s3s::dto::Metadata>) -> serde_json::Value {
    match metadata {
        Some(map) if !map.is_empty() => {
            serde_json::to_value(map).unwrap_or_else(|_| serde_json::json!({}))
        }
        _ => serde_json::json!({}),
    }
}

pub(crate) fn json_to_metadata(value: &serde_json::Value) -> Option<s3s::dto::Metadata> {
    if value.is_null() {
        return None;
    }

    match serde_json::from_value::<s3s::dto::Metadata>(value.clone()) {
        Ok(map) if !map.is_empty() => Some(map),
        _ => None,
    }
}
