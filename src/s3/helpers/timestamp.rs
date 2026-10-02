//! Conversion from the storage layer's timestamps to S3 timestamps.

use std::time::SystemTime;

use s3s::dto::Timestamp;

/// Convert a chrono timestamp into an S3 timestamp.
pub(crate) fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();

    Timestamp::from(datetime)
}
