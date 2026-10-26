//! This module translates write-side request conditions into repository
//! preconditions. It also defines the error shape that delete-marker reads
//! need.
//!
//! This module owns `If-Match` and `If-None-Match` on writes. A write
//! cannot answer them by inspecting a row the way a read does. They must
//! become a compare-and-set. The publish transaction enforces that
//! compare-and-set atomically.
//!
//! Keep repository-side execution out of this module. The translation
//! ends at [`PutCondition`]. The SQL that handles [`PutCondition`]
//! belongs to the repository.

use s3s::{
    S3Error, S3ErrorCode, S3Result,
    dto::{ETag, ETagCondition},
};

use super::super::repo::PutCondition;

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

/// Build an error for a request that addresses a delete marker.
///
/// S3 answers GET or HEAD on the latest delete marker. S3 also answers
/// version-addressed marker reads. S3 uses the usual error code. S3 also
/// carries `x-amz-delete-marker: true` and `x-amz-version-id` headers.
/// Clients use those headers to tell a versioned delete from a missing
/// key.
pub(crate) fn delete_marker_error(version_id: &str, code: S3ErrorCode) -> S3Error {
    let mut err = S3Error::new(code);

    let mut headers = http::HeaderMap::new();
    headers.insert(
        s3s::header::X_AMZ_DELETE_MARKER,
        http::HeaderValue::from_static("true"),
    );
    if let Ok(value) = http::HeaderValue::from_str(version_id) {
        headers.insert(s3s::header::X_AMZ_VERSION_ID, value);
    }
    err.set_headers(headers);

    err
}
