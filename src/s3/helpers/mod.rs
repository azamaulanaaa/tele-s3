//! Small helpers shared by the S3 request handlers, grouped by concern.
//!
//! Each submodule owns one cohesive concern and its tests; this module is
//! only a facade so callers keep importing everything from `super::helpers`.

mod acl;
mod bucket_name;
mod checksums;
mod conditional_get;
mod metadata;
mod preconditions;
mod streaming_blob;
mod tagging_header;
mod tags;
mod timestamp;

pub(crate) use acl::{canned_owner, full_control_grant};
pub(crate) use bucket_name::validate_bucket_name;
pub(crate) use checksums::{ExpectedChecksums, verify_checksums};
pub(crate) use conditional_get::check_conditional_get;
pub(crate) use metadata::{json_to_metadata, metadata_to_json};
pub(crate) use preconditions::{build_put_condition, delete_marker_error};
pub(crate) use streaming_blob::StreamingBlobExt;
pub(crate) use tagging_header::tagging_header_to_json;
pub(crate) use tags::{json_to_tag_set, tags_to_json};
pub(crate) use timestamp::chrono_to_timestamp;
