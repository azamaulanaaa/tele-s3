//! This module validates S3 general-purpose bucket names.
//!
//! This module owns the name grammar only. The bucket handlers answer
//! whether a name is free and who owns it. They call this module only to
//! reject a name that can never be valid.

use s3s::{S3Error, S3ErrorCode, S3Result};

/// Enforces the documented hard rules. A name needs 3 to 63 characters.
/// The characters must be lowercase letters, digits, dots, or hyphens.
/// The name must start and end with a letter or digit. The name must not
/// use the `xn--` prefix. The name must not resemble an IPv4 address.
pub(crate) fn validate_bucket_name(name: &str) -> S3Result<()> {
    let invalid = || S3Error::new(S3ErrorCode::InvalidBucketName);

    if name.len() < 3 || name.len() > 63 {
        return Err(invalid());
    }

    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'.' || b == b'-')
    {
        return Err(invalid());
    }

    let starts_ok = name
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    let ends_ok = name
        .bytes()
        .last()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit());
    if !starts_ok || !ends_ok {
        return Err(invalid());
    }

    if name.starts_with("xn--") {
        return Err(invalid());
    }

    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4
        && parts
            .iter()
            .all(|p| !p.is_empty() && p.len() <= 3 && p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(invalid());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs)]
    use super::*;

    #[test]
    fn valid_bucket_names_pass() {
        for name in [
            "abc",
            "my-bucket",
            "my.bucket.name",
            "123",
            "a1b2c3",
            &"a".repeat(63),
            "bucket-with-dots.and-dashes-123",
        ] {
            validate_bucket_name(name).unwrap_or_else(|_| panic!("{name:?} should be valid"));
        }
    }

    #[test]
    fn invalid_bucket_names_rejected() {
        for name in [
            "",
            "ab",
            &"a".repeat(64),
            "My-Bucket",
            "my_bucket",
            "-bucket",
            "bucket-",
            ".bucket",
            "bucket.",
            "xn--bucket",
            "192.168.0.1",
            "my bucket",
            "bucket!",
        ] {
            let err = validate_bucket_name(name).expect_err(&format!("{name:?} should be invalid"));
            assert_eq!(*err.code(), S3ErrorCode::InvalidBucketName);
        }
    }
}
