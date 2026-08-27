use anyhow::Context;
use aws_sdk_s3::{Client, primitives::ByteStream};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use config::{REGION, config};

mod config;

#[tokio::test]
async fn test_conditional_get_etag() -> anyhow::Result<()> {
    let cfg = config::<1, 1024>().await?;
    let client = Client::new(&cfg);
    let bucket = "cond-etag-test";
    let key = "file.txt";
    let content = "hello conditional";

    let loc = BucketLocationConstraint::from(REGION);
    let cc = CreateBucketConfiguration::builder().location_constraint(loc).build();
    let _ = client.create_bucket().create_bucket_configuration(cc).bucket(bucket).send().await.context("create bucket")?;

    let put = client.put_object().bucket(bucket).key(key).body(ByteStream::from_static(content.as_bytes())).send().await.context("put")?;
    let etag = put.e_tag().unwrap().to_string();
    // etag from put is without quotes, but S3 ETag header includes quotes. Use the etag as is.
    println!("etag: {}", etag);

    // Get with If-Match matching -> should succeed 200
    let _out = client.get_object().bucket(bucket).key(key).if_match(etag.clone()).send().await.context("get with if-match match")?;

    // Get with If-Match non-matching -> should be 412 PreconditionFailed
    let res = client.get_object().bucket(bucket).key(key).if_match("nonmatchingetag123").send().await;
    assert!(res.is_err(), "If-Match non-matching should be 412");
    if let Err(e) = res {
        let code = e.as_service_error().and_then(|se| se.code()).unwrap_or("");
        println!("If-Match non-matching code: {}", code);
        assert!(code.contains("PreconditionFailed") || code.contains("412"), "expected PreconditionFailed, got {}", code);
    }

    // Get with If-None-Match matching -> should be 304 NotModified
    let res = client.get_object().bucket(bucket).key(key).if_none_match(etag.clone()).send().await;
    assert!(res.is_err(), "If-None-Match matching should be 304");
    if let Err(e) = res {
        let code = e.as_service_error().and_then(|se| se.code()).unwrap_or("");
        println!("If-None-Match matching code: {}", code);
        assert!(code.contains("NotModified") || code.contains("304"), "expected NotModified, got {}", code);
    }

    // Get with If-None-Match non-matching -> should succeed 200
    let out = client.get_object().bucket(bucket).key(key).if_none_match("nonmatchingetag123").send().await.context("get with if-none-match non-matching")?;
    assert!(out.e_tag().is_some());

    // Head with If-Match
    let _ = client.head_object().bucket(bucket).key(key).if_match(etag.clone()).send().await.context("head with if-match")?;

    let res = client.head_object().bucket(bucket).key(key).if_match("badetag").send().await;
    assert!(res.is_err(), "Head If-Match non-matching should be 412");

    Ok(())
}

#[tokio::test]
async fn test_conditional_get_modified_since() -> anyhow::Result<()> {
    let cfg = config::<1, 1024>().await?;
    let client = Client::new(&cfg);
    let bucket = "cond-time-test";
    let key = "file2.txt";
    let content = "time test";

    let loc = BucketLocationConstraint::from(REGION);
    let cc = CreateBucketConfiguration::builder().location_constraint(loc).build();
    let _ = client.create_bucket().create_bucket_configuration(cc).bucket(bucket).send().await.context("create bucket")?;

    client.put_object().bucket(bucket).key(key).body(ByteStream::from_static(content.as_bytes())).send().await.context("put")?;

    // Get object to know last_modified
    let head = client.head_object().bucket(bucket).key(key).send().await.context("head")?;
    let last_modified = head.last_modified().unwrap();
    println!("last_modified: {:?}", last_modified);

    // If-Modified-Since with time after last_modified -> should be 304 (not modified)
    // Use a future time
    let future = aws_sdk_s3::primitives::DateTime::from_secs(last_modified.secs() + 3600);
    let res = client.get_object().bucket(bucket).key(key).if_modified_since(future).send().await;
    assert!(res.is_err(), "If-Modified-Since future should be 304");
    if let Err(e) = res {
        let code = e.as_service_error().and_then(|se| se.code()).unwrap_or("");
        println!("If-Modified-Since future code: {}", code);
        assert!(code.contains("NotModified") || code.contains("304"), "expected NotModified, got {}", code);
    }

    // If-Modified-Since with time before -> should succeed 200
    let past = aws_sdk_s3::primitives::DateTime::from_secs(last_modified.secs() - 3600);
    let out = client.get_object().bucket(bucket).key(key).if_modified_since(past).send().await.context("get with if-modified-since past")?;
    assert!(out.body().bytes().is_some() || true); // just check not error

    // If-Unmodified-Since with past -> should be 412
    let res = client.get_object().bucket(bucket).key(key).if_unmodified_since(past).send().await;
    assert!(res.is_err(), "If-Unmodified-Since past should be 412");
    if let Err(e) = res {
        let code = e.as_service_error().and_then(|se| se.code()).unwrap_or("");
        println!("If-Unmodified-Since past code: {}", code);
        assert!(code.contains("PreconditionFailed") || code.contains("412"), "expected PreconditionFailed, got {}", code);
    }

    // If-Unmodified-Since with future -> should succeed
    let out = client.get_object().bucket(bucket).key(key).if_unmodified_since(future).send().await.context("get with if-unmodified-since future")?;
    assert!(out.e_tag().is_some());

    Ok(())
}
