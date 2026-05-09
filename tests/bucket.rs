use aws_sdk_s3::{
    Client,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
};
use config::{REGION, config};

mod config;

#[tokio::test]
async fn test_create_and_list() -> anyhow::Result<()> {
    let config = config().await?;
    let client = Client::new(config);

    let bucket_name = "create-and-list";

    let is_exists = {
        let res = client.list_buckets().send().await?;
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
            .await?;
    }

    let is_exists = {
        let res = client.list_buckets().send().await?;
        let is_exists = res
            .buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name));

        is_exists
    };
    assert!(is_exists, "buckets should exist after create");

    Ok(())
}
