use anyhow::Context;
use aws_sdk_s3::{
    Client,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
};
use config::{REGION, config};

mod config;

#[tokio::test]
async fn test_create_bucket_and_list_bucket() -> anyhow::Result<()> {
    let config = config::<0, 0>().await?;
    let client = Client::new(&config);

    let bucket_name = "create-bucket-and-list-bucket";

    let is_exists = {
        let res = client.list_buckets().send().await.context("list bucket")?;
        let is_exists = res
            .buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name));

        is_exists
    };
    assert!(!is_exists, "buckets should not exist before create");

    {
        let location = BucketLocationConstraint::from(REGION);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(location)
            .build();

        let _ = client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket_name)
            .send()
            .await
            .context("create bucket")?;
    }

    let is_exists = {
        let res = client.list_buckets().send().await.context("list bucket")?;
        let is_exists = res
            .buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name));

        is_exists
    };
    assert!(is_exists, "buckets should exist after create");

    Ok(())
}

#[tokio::test]
async fn test_delete_bucket() -> anyhow::Result<()> {
    let config = config::<0, 0>().await?;
    let client = Client::new(&config);

    let bucket_name = "delete-bucket";

    {
        let location = BucketLocationConstraint::from(REGION);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(location)
            .build();

        let _ = client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket_name)
            .send()
            .await
            .context("create bucket")?;
    }

    let _ = client
        .delete_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .context("delete bucket")?;

    let is_exists = {
        let res = client.list_buckets().send().await.context("list bucket")?;
        let is_exists = res
            .buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name));

        is_exists
    };
    assert!(!is_exists, "buckets should not exist after delete");

    Ok(())
}
