//! Evaluation of conditional GET/HEAD request headers.

use std::time::SystemTime;

use s3s::{
    S3Error, S3ErrorCode, S3Result,
    dto::{ETag, ETagCondition, Timestamp},
};

use super::super::repo::entity::object;

pub(crate) fn check_conditional_get(
    model: &object::Model,
    if_match: Option<&ETagCondition>,
    if_none_match: Option<&ETagCondition>,
    if_modified_since: Option<&Timestamp>,
    if_unmodified_since: Option<&Timestamp>,
) -> S3Result<()> {
    fn etag_str(e: &ETag) -> &str {
        match e {
            ETag::Strong(v) | ETag::Weak(v) => v.as_str(),
        }
    }

    let model_etag = model.etag.as_deref().unwrap_or("");

    if let Some(cond) = if_match {
        match cond {
            ETagCondition::Any => {
                // Existence is guaranteed: the caller only reaches here
                // with `model` already resolved, so `*` passes.
            }
            ETagCondition::ETag(etag) => {
                if etag_str(etag) != model_etag {
                    return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
                }
            }
        }
    }

    if let Some(ts) = if_unmodified_since {
        let model_ts = Timestamp::from(SystemTime::from(model.last_modified));
        if model_ts > *ts {
            return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
        }
    }

    if let Some(cond) = if_none_match {
        match cond {
            ETagCondition::Any => {
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
            ETagCondition::ETag(etag) => {
                if etag_str(etag) == model_etag {
                    return Err(S3Error::new(S3ErrorCode::NotModified));
                }
            }
        }
    }

    if let Some(ts) = if_modified_since {
        let model_ts = Timestamp::from(SystemTime::from(model.last_modified));
        if model_ts <= *ts {
            return Err(S3Error::new(S3ErrorCode::NotModified));
        }
    }

    Ok(())
}
