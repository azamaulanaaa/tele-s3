//! Translation of write-side request conditions into repository
//! preconditions, plus the error shape delete-marker reads need.
//!
//! Owns `If-Match`/`If-None-Match` on writes, which cannot be answered by
//! inspecting a row the way reads are — they have to become a compare-and-set
//! the publish transaction enforces atomically. Keep repository-side
//! execution out of here: the translation ends at [`PutCondition`], and the
//! SQL that acts on it belongs to the repository.

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

/// Build an error for requests addressing a delete marker.
///
/// S3 answers GET/HEAD on a delete-marker latest (or version-addressed
/// marker reads) with the usual error code but carries
/// `x-amz-delete-marker: true` and `x-amz-version-id` headers so clients
/// can tell a versioned delete from a missing key.
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
