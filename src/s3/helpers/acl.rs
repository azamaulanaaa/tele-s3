//! The synthetic ACL identity every resource is reported to belong to.
//!
//! Every ACL response answers with this one owner and grant. Real ACL
//! enforcement — evaluating a request's identity against a grant — is not
//! modelled anywhere: the `PutBucketAcl`/`PutObjectAcl` handlers existence-
//! check and discard, so nothing outside this file may grow the notion of a
//! stored grant.

use s3s::dto::{Grant, Grantee, Owner};

pub(crate) fn canned_owner() -> Owner {
    Owner {
        display_name: Some("tele-s3".into()),
        id: Some("tele-s3".into()),
    }
}

pub(crate) fn full_control_grant() -> Grant {
    Grant {
        grantee: Some(Grantee {
            display_name: Some("tele-s3".into()),
            email_address: None,
            id: Some("tele-s3".into()),
            type_: s3s::dto::Type::CANONICAL_USER.to_string().into(),
            uri: None,
        }),
        permission: Some(s3s::dto::Permission::FULL_CONTROL.to_string().into()),
    }
}
