//! The expected checksums of an object or object part, and how they are
//! validated and verified against streamed digests.
//!
//! Owns both directions of the checksum concept: the *expected* side, built
//! from request headers or read back from a stored document, and the
//! comparison against digests computed while the body was being written.
//! Computing those digests is not here — it happens in the backend read and
//! write paths, which hand this module finished values.

use base64::Engine;
use s3s::{S3Error, S3ErrorCode, S3Result};

/// The four expected checksums of an object or object part.
///
/// One named type serves both directions the concept is used in:
/// [`ExpectedChecksums::new`] builds it from request headers, and
/// [`ExpectedChecksums::from_json`] reads it back out of a stored
/// checksums document (which [`ExpectedChecksums::to_json`] wrote).
///
/// Each field is an optional standard-base64 digest; `None` means the
/// client did not supply that algorithm. The fields are named rather
/// than positional because all four have the same type: a swapped pair
/// used to compile cleanly and silently corrupt checksums.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ExpectedChecksums {
    pub(crate) crc32: Option<String>,
    pub(crate) crc32c: Option<String>,
    pub(crate) sha1: Option<String>,
    pub(crate) sha256: Option<String>,
}

impl ExpectedChecksums {
    pub(crate) fn new(
        crc32: Option<String>,
        crc32c: Option<String>,
        sha1: Option<String>,
        sha256: Option<String>,
    ) -> Self {
        Self {
            crc32,
            crc32c,
            sha1,
            sha256,
        }
    }

    pub(crate) fn from_json(value: &serde_json::Value) -> Self {
        let get = |key: &str| value.get(key).and_then(|v| v.as_str()).map(String::from);

        Self::new(get("crc32"), get("crc32c"), get("sha1"), get("sha256"))
    }

    fn provided(&self) -> impl Iterator<Item = (&'static str, usize, Option<&str>)> {
        [
            ("crc32", 4usize, self.crc32.as_deref()),
            ("crc32c", 4, self.crc32c.as_deref()),
            ("sha1", 20, self.sha1.as_deref()),
            ("sha256", 32, self.sha256.as_deref()),
        ]
        .into_iter()
    }

    /// Each value must be standard base64 decoding to the algorithm's
    /// digest length.
    pub(crate) fn to_json(&self) -> S3Result<serde_json::Value> {
        let mut map = serde_json::Map::new();

        for (name, expected_bytes, provided) in self.provided() {
            if let Some(value) = provided {
                let decoded = base64::prelude::BASE64_STANDARD
                    .decode(value.trim())
                    .map_err(|_| S3Error::new(S3ErrorCode::InvalidRequest))?;

                if decoded.len() != expected_bytes {
                    return Err(S3Error::new(S3ErrorCode::InvalidRequest));
                }

                map.insert(
                    name.to_string(),
                    serde_json::Value::String(value.to_string()),
                );
            }
        }

        Ok(serde_json::Value::Object(map))
    }
}

/// Verify client-supplied checksums against streaming digests.
///
/// Each supplied value is base64-decoded (already length-validated by
/// `ExpectedChecksums::to_json`) and compared to the computed digest
/// bytes. CRC values are the 4-byte big-endian encoding per S3. Mismatch
/// -> `BadDigest` so corrupt archive uploads fail instead of being stored.
pub(crate) fn verify_checksums(
    computed_crc32: u32,
    computed_crc32c: u32,
    computed_sha1: &[u8],
    computed_sha256: &[u8],
    expected: &ExpectedChecksums,
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

    if let Some(v) = expected.crc32.as_deref() {
        compare(v, &computed_crc32.to_be_bytes())?;
    }
    if let Some(v) = expected.crc32c.as_deref() {
        compare(v, &computed_crc32c.to_be_bytes())?;
    }
    if let Some(v) = expected.sha1.as_deref() {
        compare(v, computed_sha1)?;
    }
    if let Some(v) = expected.sha256.as_deref() {
        compare(v, computed_sha256)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base64 digests of the right length for each algorithm, deliberately
    /// distinct so a mis-mapped field is visible in a failure message.
    const CRC32: &str = "3q2+7w=="; // de ad be ef
    const CRC32C: &str = "AQIDBA=="; // 01 02 03 04
    const SHA1: &str = "AQIDBAUGBwgJCgsMDQ4PEBESExQ="; // 01..14
    const SHA256: &str = "//79/Pv6+fj39vX08/Lx8O/u7ezr6uno5+bl5OPi4eA="; // ff..e0

    fn all_four() -> ExpectedChecksums {
        ExpectedChecksums {
            crc32: Some(CRC32.into()),
            crc32c: Some(CRC32C.into()),
            sha1: Some(SHA1.into()),
            sha256: Some(SHA256.into()),
        }
    }

    #[test]
    fn expected_checksums_round_trip_through_json() {
        let expected = all_four();
        let json = expected.to_json().expect("serialize");

        assert_eq!(
            json,
            serde_json::json!({"crc32": CRC32, "crc32c": CRC32C, "sha1": SHA1, "sha256": SHA256})
        );
        assert_eq!(ExpectedChecksums::from_json(&json), expected);
    }

    #[test]
    fn expected_checksums_round_trip_when_partially_populated() {
        let expected = ExpectedChecksums {
            sha256: Some(SHA256.into()),
            ..Default::default()
        };
        let json = expected.to_json().expect("serialize");

        assert_eq!(json, serde_json::json!({"sha256": SHA256}));
        assert_eq!(ExpectedChecksums::from_json(&json), expected);
    }

    #[test]
    fn all_none_checksums_serialize_to_an_empty_object() {
        let expected = ExpectedChecksums::default();
        let json = expected.to_json().expect("serialize");

        assert_eq!(json, serde_json::json!({}));
        assert_eq!(ExpectedChecksums::from_json(&json), expected);
    }

    #[test]
    fn checksum_of_the_wrong_digest_length_is_invalid_request() {
        // sha256 base64 holding only 20 bytes: right algorithm, wrong length.
        let expected = ExpectedChecksums {
            sha256: Some(SHA1.into()),
            ..Default::default()
        };
        let err = expected
            .to_json()
            .expect_err("short sha256 must be rejected");

        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn from_json_ignores_missing_and_non_string_entries() {
        let expected = ExpectedChecksums::from_json(&serde_json::json!({
            "crc32": 7,
            "sha1": SHA1,
        }));

        assert_eq!(
            expected,
            ExpectedChecksums {
                sha1: Some(SHA1.into()),
                ..Default::default()
            }
        );
    }

    #[test]
    fn verify_checksums_accepts_matching_and_rejects_mismatching() {
        verify_checksums(
            0xdead_beef,
            0x0102_0304,
            &[],
            &[],
            &ExpectedChecksums::default(),
        )
        .expect("no expectations to check");

        let expected = all_four();
        let err = verify_checksums(0xdead_beef, 0x0102_0304, &[], &[], &expected)
            .expect_err("sha1/sha256 digests do not match");
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);

        let crc = ExpectedChecksums {
            crc32: Some(CRC32.into()),
            ..Default::default()
        };
        verify_checksums(0xdead_beef, 0, &[], &[], &crc).expect("crc32 matches");
        let err = verify_checksums(0, 0, &[], &[], &crc).expect_err("crc32 differs");
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);
    }
}
