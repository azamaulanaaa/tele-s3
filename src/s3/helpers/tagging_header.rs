//! Parsing of the `x-amz-tagging` request header into the stored tag-set
//! document, and the percent-decoding it needs.

use s3s::{S3Error, S3ErrorCode, S3Result};

/// Percent-decode a `x-amz-tagging` query-param string (`Key1=Value1&...`).
///
/// Decoding is byte-oriented end to end: escaped octets are appended to a
/// byte buffer and the result is assembled as UTF-8 once, so multi-byte
/// sequences such as `%C3%A9` round-trip instead of being reinterpreted as
/// Latin-1. Never index-slice `input` (a `%` followed by non-ASCII would
/// panic on a non-char-boundary); an escape that cannot produce valid UTF-8
/// is `InvalidArgument`.
fn percent_decode(input: &str) -> S3Result<String> {
    let invalid = || S3Error::new(S3ErrorCode::InvalidArgument);
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).map_err(|_| invalid())?;
                let byte = u8::from_str_radix(hex, 16).map_err(|_| invalid())?;
                out.push(byte);
                i += 3;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| invalid())
}

/// Parse `x-amz-tagging` header (`TaggingHeader`) into the JSON tag-set
/// storage format (`[{"key":..,"value":..}]`). `None`/empty → `[]`.
/// Enforces S3 limits: max 10 tags.
pub(crate) fn tagging_header_to_json(tagging: Option<&str>) -> S3Result<serde_json::Value> {
    let header = tagging.map(str::trim).unwrap_or_default();
    if header.is_empty() {
        return Ok(serde_json::json!([]));
    }

    let mut list = Vec::new();
    for pair in header.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| S3Error::new(S3ErrorCode::InvalidArgument))?;
        let key = percent_decode(k.trim())?;
        let value = percent_decode(v.trim())?;
        if key.is_empty() || key.len() > 128 || value.len() > 256 {
            return Err(S3Error::new(S3ErrorCode::InvalidArgument));
        }
        list.push(serde_json::json!({"key": key, "value": value}));
    }

    if list.len() > 10 {
        return Err(S3Error::new(S3ErrorCode::InvalidArgument));
    }

    Ok(serde_json::Value::Array(list))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decode_round_trips_utf8() {
        assert_eq!(percent_decode("%C3%A9").unwrap(), "\u{e9}");
        assert_eq!(percent_decode("%E2%82%AC").unwrap(), "\u{20ac}");
        assert_eq!(
            percent_decode("caf%C3%A9=cr%C3%A8me").unwrap(),
            "café=crème"
        );
    }

    #[test]
    fn percent_decode_rejects_non_ascii_after_percent() {
        // Previously this panicked: `input[1..3]` was not a char boundary.
        let err = percent_decode("%\u{20ac}").expect_err("non-ASCII escape must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert_eq!(
            *percent_decode("%\u{20ac}key=v")
                .expect_err("non-ASCII escape must be rejected")
                .code(),
            S3ErrorCode::InvalidArgument
        );
    }

    #[test]
    fn percent_decode_rejects_invalid_utf8_sequences() {
        // A syntactically valid escape pair that is not valid UTF-8 on its own.
        for input in ["%C3", "%FF", "%C3%28"] {
            let err = percent_decode(input).expect_err("invalid UTF-8 must be rejected");
            assert_eq!(*err.code(), S3ErrorCode::InvalidArgument, "input {input:?}");
        }
    }

    #[test]
    fn percent_decode_rejects_malformed_hex() {
        for input in ["%ZZ", "%G0", "%0G"] {
            let err = percent_decode(input).expect_err("malformed hex must be rejected");
            assert_eq!(*err.code(), S3ErrorCode::InvalidArgument, "input {input:?}");
        }
    }

    #[test]
    fn percent_decode_handles_plus_and_plain_text() {
        assert_eq!(percent_decode("a+b").unwrap(), "a b");
        assert_eq!(percent_decode("key%20one").unwrap(), "key one");
        assert_eq!(percent_decode("plain-Text_1.0").unwrap(), "plain-Text_1.0");
        assert_eq!(percent_decode("").unwrap(), "");
    }

    #[test]
    fn percent_decode_passes_through_truncated_trailing_escape() {
        // `i + 2 < bytes.len()` is false when fewer than two bytes follow the
        // `%`, so the `%` is copied through literally instead of erroring.
        for (input, expected) in [
            ("abc%", "abc%"),
            ("%41%", "A%"),
            ("ab%4", "ab%4"),
            ("%", "%"),
        ] {
            assert_eq!(percent_decode(input).unwrap(), expected, "input {input:?}");
        }
    }

    #[test]
    fn tagging_header_parses_utf8_and_rejects_bad_escapes() {
        let parsed = tagging_header_to_json(Some("caf%C3%A9=cr%C3%A8me")).unwrap();
        assert_eq!(parsed[0]["key"], "café");
        assert_eq!(parsed[0]["value"], "crème");

        let err = tagging_header_to_json(Some("key=%\u{20ac}")).expect_err("must be rejected");
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
    }
}
