use std::time::SystemTime;

use base64::Engine;
use futures::TryStreamExt;
use s3s::{
    S3Error, S3ErrorCode, S3Result,
    dto::{ETag, ETagCondition, Grant, Grantee, Owner, StreamingBlob, Timestamp},
};

use super::repo::PutCondition;
use crate::backend::BoxedAsyncReader;

/// Convert a chrono timestamp into an S3 timestamp.
pub(crate) fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();

    Timestamp::from(datetime)
}

/// The single synthetic owner every resource is reported to belong to.
pub(crate) fn canned_owner() -> Owner {
    Owner {
        display_name: Some("tele-s3".into()),
        id: Some("tele-s3".into()),
    }
}

/// Full control granted to the synthetic owner.
pub(crate) fn full_control_grant() -> Grant {
    Grant {
        grantee: Some(Grantee {
            display_name: Some("tele-s3".into()),
            email_address: None,
            id: Some("tele-s3".into()),
            type_: s3s::dto::Type::CANONICAL_USER.to_string().into(),
            uri: None,
        }),
        permission: Some(s3s::dto::Permission::FULL_CONTROL.to_string().into()),
    }
}

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

/// Validate client-provided checksums and serialize them for storage.
/// Each value must be standard base64 decoding to the algorithm's digest
/// length.
pub(crate) fn checksums_to_json(
    crc32: Option<String>,
    crc32c: Option<String>,
    sha1: Option<String>,
    sha256: Option<String>,
) -> S3Result<serde_json::Value> {
    let mut map = serde_json::Map::new();

    for (name, expected_bytes, provided) in [
        ("crc32", 4usize, crc32),
        ("crc32c", 4, crc32c),
        ("sha1", 20, sha1),
        ("sha256", 32, sha256),
    ] {
        if let Some(value) = provided {
            let decoded = base64::prelude::BASE64_STANDARD
                .decode(value.trim())
                .map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest))?;

            if decoded.len() != expected_bytes {
                return Err(S3Error::new(S3ErrorCode::InvalidRequest));
            }

            map.insert(name.to_string(), serde_json::Value::String(value));
        }
    }

    Ok(serde_json::Value::Object(map))
}

/// Verify client-supplied checksums against streaming digests.
///
/// Each supplied value is base64-decoded (already length-validated by
/// `checksums_to_json`) and compared to the computed digest bytes. CRC
/// values are the 4-byte big-endian encoding per S3. Mismatch -> `BadDigest`
/// so corrupt archive uploads fail instead of being stored.
#[allow(clippy::too_many_arguments)]
pub(crate) fn verify_checksums(
    computed_crc32: u32,
    computed_crc32c: u32,
    computed_sha1: &[u8],
    computed_sha256: &[u8],
    expected_crc32: Option<&str>,
    expected_crc32c: Option<&str>,
    expected_sha1: Option<&str>,
    expected_sha256: Option<&str>,
) -> S3Result<()> {
    let compare = |expected: &str, computed: &[u8]| -> S3Result<()> {
        let decoded = base64::prelude::BASE64_STANDARD
            .decode(expected.trim())
            .map_err(|_| S3Error::new(S3ErrorCode::InvalidDigest))?;
        if decoded.as_slice() != computed {
            return Err(S3Error::new(S3ErrorCode::BadDigest));
        }
        Ok(())
    };

    if let Some(v) = expected_crc32 {
        compare(v, &computed_crc32.to_be_bytes())?;
    }
    if let Some(v) = expected_crc32c {
        compare(v, &computed_crc32c.to_be_bytes())?;
    }
    if let Some(v) = expected_sha1 {
        compare(v, computed_sha1)?;
    }
    if let Some(v) = expected_sha256 {
        compare(v, computed_sha256)?;
    }
    Ok(())
}
/// Extract stored checksum values as (crc32, crc32c, sha1, sha256).
pub(crate) fn json_to_checksum_fields(
    value: &serde_json::Value,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let get = |key: &str| value.get(key).and_then(|v| v.as_str()).map(String::from);

    (get("crc32"), get("crc32c"), get("sha1"), get("sha256"))
}

pub(crate) fn tags_to_json(tagging: s3s::dto::Tagging) -> serde_json::Value {
    let list: Vec<serde_json::Value> = tagging
        .tag_set
        .into_iter()
        .map(|tag| {
            serde_json::json!({
                "key": tag.key.unwrap_or_default(),
                "value": tag.value.unwrap_or_default(),
            })
        })
        .collect();

    serde_json::Value::Array(list)
}

pub(crate) fn json_to_tag_set(value: &serde_json::Value) -> Vec<s3s::dto::Tag> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    let key = entry.get("key")?.as_str()?.to_string();
                    let tag_value = entry.get("value")?.as_str()?.to_string();

                    Some(s3s::dto::Tag {
                        key: Some(key),
                        value: Some(tag_value),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Translate If-Match / If-None-Match headers into a write precondition.
pub(crate) fn build_put_condition(
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
) -> S3Result<PutCondition> {
    if if_match.is_some() && if_none_match.is_some() {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument));
    }

    fn etag_value(e: &ETag) -> String {
        match e {
            ETag::Strong(v) | ETag::Weak(v) => v.clone(),
        }
    }

    Ok(match (if_match, if_none_match) {
        (Some(ETagCondition::ETag(e)), _) => PutCondition::IfMatch(etag_value(e)),
        (Some(ETagCondition::Any), _) => PutCondition::IfMatchAny,
        (_, Some(ETagCondition::Any)) => PutCondition::IfNoneMatchAny,
        (_, Some(ETagCondition::ETag(e))) => PutCondition::IfNoneMatch(etag_value(e)),
        _ => PutCondition::None,
    })
}

/// Check conditional GET/HEAD headers. Returns Err with appropriate S3ErrorCode if condition fails.
/// For `If-Match` / `If-Unmodified-Since` failures -> PreconditionFailed (412)
/// For `If-None-Match` / `If-Modified-Since` not modified -> NotModified (304)
pub(crate) fn check_conditional_get(
    model: &super::repo::entity::object::Model,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<()> {
    // Helper to extract etag string from ETagCondition
    fn etag_str(e: &ETag) -> &str {
        match e {
            ETag::Strong(v) | ETag::Weak(v) => v.as_str(),
        }
    }

    // Helper to get model etag as &str (empty if None)
    let model_etag = model.etag.as_deref().unwrap_or("");

    // If-Match
    if let Some(cond) = if_match {
        match cond {
            ETagCondition::Any => {
                // If-Match: * requires object exists (it does, we have model)
                // so pass
            }
            ETagCondition::ETag(etag) => {
                if etag_str(etag) != model_etag {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
        }
    }

    // If-Unmodified-Since
    if let Some(ts) = if_unmodified_since {
        let model_ts = Timestamp::from(SystemTime::from(model.last_modified));
        if model_ts > *ts {
            return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
        }
    }

    // If-None-Match
    if let Some(cond) = if_none_match {
        match cond {
            ETagCondition::Any => {
                // If-None-Match: * with existing object -> NotModified
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
            ETagCondition::ETag(etag) => {
                if etag_str(etag) == model_etag {
                    return Err(S3Error::new(S3ErrorCode::NotModified));
                }
            }
        }
    }

    // If-Modified-Since
    if let Some(ts) = if_modified_since {
        let model_ts = Timestamp::from(SystemTime::from(model.last_modified));
        if model_ts <= *ts {
            return Err(S3Error::new(S3ErrorCode::NotModified));
        }
    }

    Ok(())
}

pub(crate) trait StreamingBlobExt {
    fn into_boxed_reader(self) -> BoxedAsyncReader;
}

impl StreamingBlobExt for StreamingBlob {
    fn into_boxed_reader(self) -> BoxedAsyncReader {
        let stream = self.map_err(std::io::Error::other).into_async_read();
        Box::pin(stream)
    }
}
