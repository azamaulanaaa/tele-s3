//! Conversion from the storage layer's timestamps to S3 timestamps.
//!
//! One direction only: the repository stores `chrono::DateTime<Utc>` and
//! responses speak `s3s::dto::Timestamp`. Parsing a timestamp *out* of a
//! request is `s3s`' job — the crate only ever hands these values back out.

use std::time::SystemTime;

use s3s::dto::Timestamp;

pub(crate) fn chrono_to_timestamp(datetime: chrono::DateTime<chrono::Utc>) -> Timestamp {
    let datetime: SystemTime = datetime.into();

    Timestamp::from(datetime)
}
