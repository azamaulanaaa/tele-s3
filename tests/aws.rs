use anyhow::Context;
use aws_sdk_s3::{
    Client,
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
};
use config::{REGION, config};
use tokio::io::AsyncReadExt;

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

    let err = {
        let res = client.delete_bucket().bucket(bucket_name).send().await;
        res.err()
    };
    assert_eq!(
        err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
        Some("NoSuchBucket".to_string())
    );

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

#[tokio::test]
async fn test_put_object_and_list_objects() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "put-object-and-list-objects";

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

    let object_name = "test_name";
    let object_content = "test_content";

    let objects = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects.len(), 0);

    let _ = client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from_static(object_content.as_bytes()))
        .send()
        .await
        .context("put object")?;

    let objects = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key(), Some(object_name));

    Ok(())
}

#[tokio::test]
async fn test_put_object_and_list_objects_v2() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "put-object-and-list-objects-v2";

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

    let object_name = "test_name";
    let object_content = "test_content";

    let objects = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects.len(), 0);

    let _ = client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from_static(object_content.as_bytes()))
        .send()
        .await
        .context("put object")?;

    let objects = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key(), Some(object_name));

    Ok(())
}

#[tokio::test]
async fn test_put_object_and_get_object() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "put-object-and-get-objects";

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

    let object_name = "test_name";
    let object_content = "test_content";

    let err = {
        let res = client
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await;
        res.err()
    };
    assert_eq!(
        err.map(|e| e.code().map(|e| e.to_owned())).flatten(),
        Some("NoSuchKey".to_string())
    );

    let _ = client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from_static(object_content.as_bytes()))
        .send()
        .await
        .context("put object")?;

    let output_content = {
        let res = client
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await
            .context("get object")?;
        let mut output_content = String::new();
        let _ = res
            .body
            .into_async_read()
            .read_to_string(&mut output_content)
            .await
            .context("read to string")?;
        output_content
    };
    assert_eq!(output_content, object_content);

    Ok(())
}

#[tokio::test]
async fn test_delete_object() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "delete-object";

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

    let object_name = "test_name";
    let object_content = "test_content";

    let _ = client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from_static(object_content.as_bytes()))
        .send()
        .await
        .context("put object")?;

    let _ = client
        .delete_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .context("delete object")?;

    let err = {
        let res = client
            .get_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await;
        res.err()
    };
    assert_eq!(
        err.map(|e| e.code().map(|e| e.to_owned())).flatten(),
        Some("NoSuchKey".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_list_objects_with_prefix() -> anyhow::Result<()> {
    let config = config::<2, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-with-prefix";

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

    let prefix = "prefix/";
    let objects_data = [
        ["prefix/item", "test_content"],
        ["prefix_item", "test_content"],
    ];

    for &[key, content] in objects_data.iter() {
        let _ = client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .context("put object")?;
    }

    let objects_list = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .prefix(prefix)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects_list.len(), 1);
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_with_prefix() -> anyhow::Result<()> {
    let config = config::<2, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-v2-with-prefix";

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

    let prefix = "prefix/";
    let objects_data = [
        ["prefix/item", "test_content"],
        ["prefix_item", "test_content"],
    ];

    for &[key, content] in objects_data.iter() {
        let _ = client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .context("put object")?;
    }

    let objects_list = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(prefix)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };
    assert_eq!(objects_list.len(), 1);
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_with_prefix_and_delimiter() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-v2-with-prefix";

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

    let prefix = "prefix/";
    let objects_data = [
        ["prefix/item", "test_content"],
        ["prefix/sub_item/item", "test_content"],
        ["prefix_item", "test_content"],
    ];

    for &[key, content] in objects_data.iter() {
        let _ = client
            .put_object()
            .bucket(bucket_name)
            .key(key)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .context("put object")?;
    }

    let objects_list = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(prefix)
            .delimiter("/")
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        res.contents().to_owned()
    };

    assert_eq!(objects_list.len(), 1);
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    Ok(())
}
