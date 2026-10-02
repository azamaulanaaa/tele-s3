//! Evaluation of conditional GET/HEAD request headers.

use std::time::SystemTime;

use s3s::{
    S3Error, S3ErrorCode, S3Result,
    dto::{ETag, ETagCondition, Timestamp},
};

use super::super::repo::entity::object;

/// Check conditional GET/HEAD headers. Returns Err with appropriate S3ErrorCode if condition fails.
/// For `If-Match` / `If-Unmodified-Since` failures -> PreconditionFailed (412)
/// For `If-None-Match` / `If-Modified-Since` not modified -> NotModified (304)
pub(crate) fn check_conditional_get(
    model: &object::Model,
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
