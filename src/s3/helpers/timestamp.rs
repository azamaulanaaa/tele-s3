//! This module converts storage-layer timestamps to S3 timestamps.
//!
//! This conversion goes one way only. The repository stores
//! `chrono::DateTime<Utc>`. Responses use `s3s::dto::Timestamp`. `s3s`
//! parses a timestamp out of a request. This crate only returns these
//! values.

use std::time::SystemTime;

use s3s::dto::Timestamp;

pub(crate) fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();

    Timestamp::from(datetime)
}
