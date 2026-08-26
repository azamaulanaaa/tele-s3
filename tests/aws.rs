use anyhow::Context;
use aws_sdk_s3::{
    Client,
    error::ProvideErrorMetadata,
    primitives::ByteStream,
    types::{
        BucketLocationConstraint, CompletedMultipartUpload, CompletedPart,
        CreateBucketConfiguration,
    },
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

        res.buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name))
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

        res.buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name))
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
        err.and_then(|e| e.code().map(|c| c.to_owned())),
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

        res.buckets()
            .iter()
            .any(|bucket| bucket.name().is_some_and(|name| name == bucket_name))
    };
    assert!(!is_exists, "buckets should not exist after delete");

    Ok(())
}

#[tokio::test]
async fn test_head_bucket() -> anyhow::Result<()> {
    let config = config::<0, 0>().await?;
    let client = Client::new(&config);

    let bucket_name = "head-bucket";

    let err = {
        let res = client.head_bucket().bucket(bucket_name).send().await;
        res.err()
    };
    assert_eq!(
        err.and_then(|e| e.code().map(|c| c.to_owned())),
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

    let res = client
        .head_bucket()
        .bucket(bucket_name)
        .send()
        .await
        .context("head bucket")?;

    assert_eq!(res.bucket_region(), Some(REGION));

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
async fn test_put_object_and_head_object() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "put-object-and-head-objects";

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
            .head_object()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await;
        res.err()
    };
    assert_eq!(
        err.and_then(|e| e.code().map(|e| e.to_owned())),
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

    client
        .head_object()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .context("head object")?;

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
        err.and_then(|e| e.code().map(|e| e.to_owned())),
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
        err.and_then(|e| e.code().map(|e| e.to_owned())),
        Some("NoSuchKey".to_string())
    );

    Ok(())
}

#[tokio::test]
async fn test_copy_object() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "copy-object";

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
    let object_destination_name = "text_name_2";
    let object_content = "test_content";
    let copy_source = format!("{}/{}", bucket_name, object_name);

    let err = {
        let res = client
            .copy_object()
            .bucket(bucket_name)
            .key(object_destination_name)
            .copy_source(&copy_source)
            .send()
            .await;
        res.err()
    };
    assert_eq!(
        err.and_then(|e| e.code().map(|e| e.to_owned())),
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

    let _ = client
        .copy_object()
        .bucket(bucket_name)
        .key(object_destination_name)
        .copy_source(&copy_source)
        .send()
        .await
        .context("copy object")?;

    let output_content = {
        let res = client
            .get_object()
            .bucket(bucket_name)
            .key(object_destination_name)
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
async fn test_list_objects_with_prefix_and_delimiter() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-with-prefix-and-delimiter";

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
    let common_prefix = "prefix/sub_item/";
    let delimiter = "/";
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

    let (objects_list, common_prefix_list) = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .prefix(prefix)
            .delimiter(delimiter)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        (res.contents().to_owned(), res.common_prefixes().to_owned())
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    assert_eq!(
        common_prefix_list.len(),
        1,
        "length of common prefix list is not 1"
    );
    assert_eq!(common_prefix_list[0].prefix(), Some(common_prefix));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_with_prefix_and_delimiter() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-v2-with-prefix-and-delimiter";

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
    let common_prefix = "prefix/sub_item/";
    let delimiter = "/";
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

    let (objects_list, common_prefix_list) = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(prefix)
            .delimiter(delimiter)
            .max_keys(10)
            .send()
            .await
            .context("list objects")?;
        (res.contents().to_owned(), res.common_prefixes().to_owned())
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    assert_eq!(
        common_prefix_list.len(),
        1,
        "length of common prefix list is not 1"
    );
    assert_eq!(common_prefix_list[0].prefix(), Some(common_prefix));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_with_next_marker() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-with-next-marker";

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

    let (objects_list, next_marker) = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .max_keys(1)
            .send()
            .await
            .context("list objects")?;
        (
            res.contents().to_owned(),
            res.next_marker().map(|v| v.to_owned()),
        )
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    let next_marker = next_marker.expect("next marker is missing");

    let objects_list = {
        let res = client
            .list_objects()
            .bucket(bucket_name)
            .max_keys(1)
            .marker(next_marker)
            .send()
            .await
            .context("list objects with marker")?;
        res.contents().to_owned()
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[1][0]));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_with_continuation_token() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-v2-with-continuation-token";

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

    let (objects_list, continuation_token) = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .max_keys(1)
            .send()
            .await
            .context("list objects")?;
        (
            res.contents().to_owned(),
            res.next_continuation_token().map(|v| v.to_owned()),
        )
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[0][0]));

    let continuation_token = continuation_token.expect("continuation token is missing");

    let objects_list = {
        let res = client
            .list_objects_v2()
            .bucket(bucket_name)
            .max_keys(1)
            .continuation_token(continuation_token)
            .send()
            .await
            .context("list objects with continuation token")?;
        res.contents().to_owned()
    };

    assert_eq!(objects_list.len(), 1, "length of object list is not 1");
    assert_eq!(objects_list[0].key(), Some(objects_data[1][0]));

    Ok(())
}

#[tokio::test]
async fn test_list_objects_v2_with_start_after() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-objects-v2-with-start-after";

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
            .max_keys(2)
            .start_after(objects_data[0][0])
            .send()
            .await
            .context("list objects with start after")?;
        res.contents().to_owned()
    };

    assert_eq!(objects_list.len(), 2, "length of object list is not 2");
    assert_eq!(objects_list[0].key(), Some(objects_data[1][0]));
    assert_eq!(objects_list[1].key(), Some(objects_data[2][0]));

    Ok(())
}

#[tokio::test]
async fn test_create_multipart_upload() -> anyhow::Result<()> {
    let config = config::<2, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "multipart-upload";

    {
        let err = {
            let res = client
                .create_multipart_upload()
                .bucket("no-such-multipart-bucket")
                .key("test")
                .send()
                .await;
            res.err()
        };
        assert_eq!(
            err.and_then(|e| e.code().map(|e| e.to_owned())),
            Some("NoSuchBucket".to_string())
        );
    }

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

    let object_name = "test";
    let objects_content = ["part1", "part2"];

    let upload_id = {
        let res = client
            .create_multipart_upload()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await
            .context("create multipart upload")?;

        res.upload_id.context("missing upload id")?
    };

    let mut completed_part = CompletedMultipartUpload::builder();
    for (idx, content) in objects_content.iter().enumerate() {
        let part_number = (idx as i32) + 1;
        let _ = client
            .upload_part()
            .bucket(bucket_name)
            .upload_id(&upload_id)
            .key(object_name)
            .part_number(part_number)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .context("upload part")?;

        completed_part =
            completed_part.parts(CompletedPart::builder().part_number(part_number).build())
    }
    let completed_part = completed_part.build();

    let _ = client
        .complete_multipart_upload()
        .bucket(bucket_name)
        .upload_id(upload_id)
        .key(object_name)
        .multipart_upload(completed_part)
        .send()
        .await
        .context("complete multipart upload")?;

    Ok(())
}

#[tokio::test]
async fn test_list_parts() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-parts";

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

    let object_name = "test";
    let objects_content = ["part1", "part2content"];

    let upload_id = {
        let res = client
            .create_multipart_upload()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await
            .context("create multipart upload")?;

        res.upload_id.context("missing upload id")?
    };

    for (idx, content) in objects_content.iter().enumerate() {
        let part_number = (idx as i32) + 1;
        let _ = client
            .upload_part()
            .bucket(bucket_name)
            .upload_id(&upload_id)
            .key(object_name)
            .part_number(part_number)
            .body(ByteStream::from_static(content.as_bytes()))
            .send()
            .await
            .context("upload part")?;
    }

    // Full listing
    {
        let res = client
            .list_parts()
            .bucket(bucket_name)
            .key(object_name)
            .upload_id(&upload_id)
            .send()
            .await
            .context("list parts")?;

        let parts = res.parts();
        assert_eq!(parts.len(), 2, "length of parts is not 2");
        assert_eq!(parts[0].part_number(), Some(1));
        assert_eq!(parts[0].size(), Some(objects_content[0].len() as i64));
        assert_eq!(parts[1].part_number(), Some(2));
        assert_eq!(parts[1].size(), Some(objects_content[1].len() as i64));
        assert_eq!(
            res.is_truncated(),
            Some(false),
            "full listing should not be truncated"
        );
    }

    // Paginated listing
    let (is_truncated, next_marker) = {
        let res = client
            .list_parts()
            .bucket(bucket_name)
            .key(object_name)
            .upload_id(&upload_id)
            .max_parts(1)
            .send()
            .await
            .context("list parts first page")?;

        let parts = res.parts();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].part_number(), Some(1));

        (
            res.is_truncated(),
            res.next_part_number_marker().map(str::to_owned),
        )
    };

    assert_eq!(is_truncated, Some(true), "first page should be truncated");
    assert_eq!(
        next_marker.as_deref(),
        Some("1"),
        "next marker should be last returned part"
    );

    let second_page = {
        let res = client
            .list_parts()
            .bucket(bucket_name)
            .key(object_name)
            .upload_id(&upload_id)
            .max_parts(1)
            .part_number_marker(next_marker.unwrap())
            .send()
            .await
            .context("list parts second page")?;

        res.parts().to_owned()
    };

    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].part_number(), Some(2));

    Ok(())
}

#[tokio::test]
async fn test_list_multipart_uploads() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "list-multipart-uploads";
    let other_bucket_name = "list-multipart-uploads-other";

    for name in [bucket_name, other_bucket_name] {
        let location = BucketLocationConstraint::from(REGION);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(location)
            .build();

        let _ = client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(name)
            .send()
            .await
            .context("create bucket")?;
    }

    let mut created: Vec<(String, String)> = Vec::new();
    for object_name in ["a", "b"] {
        let res = client
            .create_multipart_upload()
            .bucket(bucket_name)
            .key(object_name)
            .send()
            .await
            .context("create multipart upload")?;

        created.push((
            object_name.to_string(),
            res.upload_id.expect("missing upload id"),
        ));
    }

    let _ = client
        .create_multipart_upload()
        .bucket(other_bucket_name)
        .key("c")
        .send()
        .await
        .context("create multipart upload in other bucket")?;

    // Bucket-scoped listing
    {
        let res = client
            .list_multipart_uploads()
            .bucket(bucket_name)
            .send()
            .await
            .context("list multipart uploads")?;

        let uploads = res.uploads();
        assert_eq!(uploads.len(), 2, "length of uploads is not 2");
        assert_eq!(uploads[0].key(), Some("a"));
        assert_eq!(uploads[1].key(), Some("b"));

        for (idx, upload) in uploads.iter().enumerate() {
            assert_eq!(
                upload.upload_id(),
                Some(created[idx].1.as_str()),
                "upload id mismatch"
            );
        }
    }

    // Prefix filter
    let (prefix_uploads, is_truncated) = {
        let res = client
            .list_multipart_uploads()
            .bucket(bucket_name)
            .prefix("a")
            .send()
            .await
            .context("list multipart uploads with prefix")?;

        (res.uploads().to_owned(), res.is_truncated())
    };

    assert_eq!(prefix_uploads.len(), 1);
    assert_eq!(prefix_uploads[0].key(), Some("a"));
    assert_eq!(is_truncated, Some(false));

    // Max uploads truncation
    let is_truncated = {
        let res = client
            .list_multipart_uploads()
            .bucket(bucket_name)
            .max_uploads(1)
            .send()
            .await
            .context("list multipart uploads with max uploads")?;

        assert_eq!(res.uploads().len(), 1);
        res.is_truncated()
    };

    assert_eq!(
        is_truncated,
        Some(true),
        "should be truncated by max uploads"
    );

    Ok(())
}

#[tokio::test]
async fn test_upload_part_copy() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "upload-part-copy";
    let source_name = "source";
    let source_content = "hello world";

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
        .put_object()
        .bucket(bucket_name)
        .key(source_name)
        .body(ByteStream::from_static(source_content.as_bytes()))
        .send()
        .await
        .context("put source object")?;

    // Full-object copy as a single part
    {
        let upload_id = {
            let res = client
                .create_multipart_upload()
                .bucket(bucket_name)
                .key("dest-full")
                .send()
                .await
                .context("create multipart upload")?;
            res.upload_id.context("missing upload id")?
        };

        let copy_source = format!("{}/{}", bucket_name, source_name);

        let e_tag = {
            let res = client
                .upload_part_copy()
                .bucket(bucket_name)
                .key("dest-full")
                .upload_id(&upload_id)
                .part_number(1)
                .copy_source(&copy_source)
                .send()
                .await
                .context("upload part copy")?;

            // Option A: the slice digest is computed by streaming the shared
            // bytes back once, so the response carries a real ETag.
            res.copy_part_result
                .and_then(|r| r.e_tag)
                .expect("shared slice should report a computed ETag")
        };

        let completed_part = CompletedPart::builder()
            .e_tag(e_tag)
            .part_number(1)
            .build();

        let completed = CompletedMultipartUpload::builder()
            .parts(completed_part)
            .build();

        let _ = client
            .complete_multipart_upload()
            .bucket(bucket_name)
            .key("dest-full")
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .context("complete multipart upload")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("dest-full")
            .send()
            .await
            .context("get copied object")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(
            data.into_bytes().as_ref(),
            source_content.as_bytes(),
            "full copy content mismatch"
        );
    }

    // Ranged copy: bytes 6-10 of "hello world" is "world"
    {
        let upload_id = {
            let res = client
                .create_multipart_upload()
                .bucket(bucket_name)
                .key("dest-ranged")
                .send()
                .await
                .context("create multipart upload")?;
            res.upload_id.context("missing upload id")?
        };

        let copy_source = format!("{}/{}", bucket_name, source_name);

        let _ = client
            .upload_part_copy()
            .bucket(bucket_name)
            .key("dest-ranged")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(&copy_source)
            .copy_source_range("bytes=6-10")
            .send()
            .await
            .context("upload ranged part copy")?;

        let completed_part = CompletedPart::builder().part_number(1).build();
        let completed = CompletedMultipartUpload::builder()
            .parts(completed_part)
            .build();

        let _ = client
            .complete_multipart_upload()
            .bucket(bucket_name)
            .key("dest-ranged")
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .context("complete ranged multipart upload")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("dest-ranged")
            .send()
            .await
            .context("get ranged copy object")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(data.into_bytes().as_ref(), b"world", "range copy mismatch");
    }

    Ok(())
}

#[tokio::test]
async fn test_copied_object_survives_sibling_deletion() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "refcount-copy";
    let source_content = "hello world";

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
        .put_object()
        .bucket(bucket_name)
        .key("source")
        .body(ByteStream::from_static(source_content.as_bytes()))
        .send()
        .await
        .context("put source object")?;

    let copy_source = format!("{}/source", bucket_name);

    // Deleting one alias must not corrupt the other.
    {
        let _ = client
            .copy_object()
            .bucket(bucket_name)
            .key("alias")
            .copy_source(&copy_source)
            .send()
            .await
            .context("copy object")?;

        let _ = client
            .delete_object()
            .bucket(bucket_name)
            .key("alias")
            .send()
            .await
            .context("delete alias")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("source")
            .send()
            .await
            .context("source should survive alias deletion")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(
            data.into_bytes().as_ref(),
            source_content.as_bytes(),
            "source content mismatch after sibling deletion"
        );
    }

    // Overwriting a copy must not corrupt its source either.
    {
        let _ = client
            .copy_object()
            .bucket(bucket_name)
            .key("alias")
            .copy_source(&copy_source)
            .send()
            .await
            .context("copy object again")?;

        let _ = client
            .put_object()
            .bucket(bucket_name)
            .key("alias")
            .body(ByteStream::from_static(b"replacement".as_slice()))
            .send()
            .await
            .context("overwrite alias")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("source")
            .send()
            .await
            .context("source should survive alias overwrite")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(
            data.into_bytes().as_ref(),
            source_content.as_bytes(),
            "source content mismatch after alias overwrite"
        );
    }

    // The final delete of the last reference removes the blob for good.
    {
        let _ = client
            .delete_object()
            .bucket(bucket_name)
            .key("source")
            .send()
            .await
            .context("delete source")?;

        let err = {
            let res = client
                .get_object()
                .bucket(bucket_name)
                .key("source")
                .send()
                .await;
            res.err()
        };

        assert_eq!(
            err.map(|e| e.code().map(|e| e.to_owned())).flatten(),
            Some("NoSuchKey".to_string()),
            "deleted source should be gone"
        );

        // The untouched alias keeps working on its own content.
        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("alias")
            .send()
            .await
            .context("alias should survive unrelated deletion")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(data.into_bytes().as_ref(), b"replacement");
    }

    Ok(())
}

#[tokio::test]
async fn test_upload_part_copy_shares_without_harming_source() -> anyhow::Result<()> {
    let config = config::<3, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "upload-part-copy-share";
    let source_content = "hello world";

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
        .put_object()
        .bucket(bucket_name)
        .key("source")
        .body(ByteStream::from_static(source_content.as_bytes()))
        .send()
        .await
        .context("put source object")?;

    let copy_source = format!("{}/source", bucket_name);

    // Ranged copy-part into an upload that is then aborted: the source
    // must remain fully intact.
    {
        let upload_id = {
            let res = client
                .create_multipart_upload()
                .bucket(bucket_name)
                .key("dest-abort")
                .send()
                .await
                .context("create multipart upload")?;
            res.upload_id.context("missing upload id")?
        };

        let _ = client
            .upload_part_copy()
            .bucket(bucket_name)
            .key("dest-abort")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(&copy_source)
            .copy_source_range("bytes=6-10")
            .send()
            .await
            .context("ranged part copy")?;

        let _ = client
            .abort_multipart_upload()
            .bucket(bucket_name)
            .key("dest-abort")
            .upload_id(upload_id)
            .send()
            .await
            .context("abort multipart upload")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("source")
            .send()
            .await
            .context("source should survive aborted shared copy")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(
            data.into_bytes().as_ref(),
            source_content.as_bytes(),
            "source corrupted by aborted shared copy"
        );
    }

    // Completing a ranged copy and then deleting the source: the
    // destination's shared slices must survive.
    {
        let upload_id = {
            let res = client
                .create_multipart_upload()
                .bucket(bucket_name)
                .key("dest-survives")
                .send()
                .await
                .context("create multipart upload")?;
            res.upload_id.context("missing upload id")?
        };

        let _ = client
            .upload_part_copy()
            .bucket(bucket_name)
            .key("dest-survives")
            .upload_id(&upload_id)
            .part_number(1)
            .copy_source(&copy_source)
            .copy_source_range("bytes=6-10")
            .send()
            .await
            .context("ranged part copy")?;

        let completed = CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).build())
            .build();

        let _ = client
            .complete_multipart_upload()
            .bucket(bucket_name)
            .key("dest-survives")
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .context("complete multipart upload")?;

        let _ = client
            .delete_object()
            .bucket(bucket_name)
            .key("source")
            .send()
            .await
            .context("delete source")?;

        let out = client
            .get_object()
            .bucket(bucket_name)
            .key("dest-survives")
            .send()
            .await
            .context("destination should survive source deletion")?;

        let data = out.body.collect().await.context("collect body")?;
        assert_eq!(
            data.into_bytes().as_ref(),
            b"world",
            "shared slices lost after source deletion"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_conditional_writes() -> anyhow::Result<()> {
    let config = config::<2, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "conditional-writes";

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

    // Seed the object and capture its ETag.
    let etag_v1 = {
        let res = client
            .put_object()
            .bucket(bucket_name)
            .key("k")
            .body(ByteStream::from_static(b"v1".as_slice()))
            .send()
            .await
            .context("put v1")?;
        res.e_tag.context("missing etag")?
    };

    // If-Match with the current ETag succeeds and rotates the ETag.
    let etag_v2 = {
        let res = client
            .put_object()
            .bucket(bucket_name)
            .key("k")
            .if_match(&etag_v1)
            .body(ByteStream::from_static(b"v2".as_slice()))
            .send()
            .await
            .context("put v2 with matching If-Match")?;
        res.e_tag.context("missing etag")?
    };

    assert_ne!(etag_v1, etag_v2, "content changed so ETag must rotate");

    // Stale If-Match is rejected.
    {
        let err = {
            let res = client
                .put_object()
                .bucket(bucket_name)
                .key("k")
                .if_match(&etag_v1)
                .body(ByteStream::from_static(b"stale".as_slice()))
                .send()
                .await;
            res.err()
        };

        assert_eq!(
            err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
            Some("PreconditionFailed".to_string())
        );
    }

    // If-None-Match: * creates only once.
    {
        client
            .put_object()
            .bucket(bucket_name)
            .key("fresh")
            .if_none_match("*")
            .body(ByteStream::from_static(b"first".as_slice()))
            .send()
            .await
            .context("create-only put should succeed on missing key")?;

        let err = {
            let res = client
                .put_object()
                .bucket(bucket_name)
                .key("fresh")
                .if_none_match("*")
                .body(ByteStream::from_static(b"second".as_slice()))
                .send()
                .await;
            res.err()
        };

        assert_eq!(
            err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
            Some("PreconditionFailed".to_string())
        );
    }

    // If-Match against a missing object reports NoSuchKey.
    {
        let err = {
            let res = client
                .put_object()
                .bucket(bucket_name)
                .key("missing")
                .if_match(&etag_v1)
                .body(ByteStream::from_static(b"x".as_slice()))
                .send()
                .await;
            res.err()
        };

        assert_eq!(
            err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
            Some("NoSuchKey".to_string())
        );
    }

    // Conditional completion: completing over an existing object with a
    // mismatched If-Match fails without consuming the upload.
    {
        let upload_id = {
            let res = client
                .create_multipart_upload()
                .bucket(bucket_name)
                .key("k")
                .send()
                .await
                .context("create multipart upload")?;
            res.upload_id.expect("missing upload id")
        };

        let completed = CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).build())
            .build();

        let err = {
            let res = client
                .complete_multipart_upload()
                .bucket(bucket_name)
                .key("k")
                .upload_id(upload_id.clone())
                .if_match(&etag_v1)
                .multipart_upload(completed)
                .send()
                .await;
            res.err()
        };

        assert_eq!(
            err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
            Some("PreconditionFailed".to_string()),
            "stale If-Match must reject completion"
        );

        // The upload survives a rejected completion; an unconditional
        // retry succeeds.
        let completed = CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).build())
            .build();

        let _ = client
            .complete_multipart_upload()
            .bucket(bucket_name)
            .key("k")
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .context("unconditional completion should succeed");
    }

    Ok(())
}

#[tokio::test]
async fn test_acl_stubs() -> anyhow::Result<()> {
    let config = config::<1, 1024>().await?;
    let client = Client::new(&config);

    let bucket_name = "acl-stubs";
    let object_name = "obj";

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
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(ByteStream::from_static(b"data".as_slice()))
        .send()
        .await
        .context("put object")?;

    // Bucket ACL round-trip: accepted on put, canned owner + grant on get.
    let _ = client
        .put_bucket_acl()
        .bucket(bucket_name)
        .send()
        .await
        .context("put bucket acl")?;

    let bucket_acl = client
        .get_bucket_acl()
        .bucket(bucket_name)
        .send()
        .await
        .context("get bucket acl")?;

    assert!(
        bucket_acl.owner().is_some(),
        "bucket acl should carry an owner"
    );
    assert!(
        !bucket_acl.grants().is_empty(),
        "bucket acl should carry at least one grant"
    );

    // Object ACL round-trip.
    let _ = client
        .put_object_acl()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .context("put object acl")?;

    let object_acl = client
        .get_object_acl()
        .bucket(bucket_name)
        .key(object_name)
        .send()
        .await
        .context("get object acl")?;

    assert!(object_acl.owner().is_some(), "object acl should have owner");
    assert!(!object_acl.grants().is_empty());

    // ACLs against missing resources still report the right errors.
    let err = {
        let res = client
            .get_object_acl()
            .bucket(bucket_name)
            .key("no-such-object")
            .send()
            .await;
        res.err()
    };
    assert_eq!(
        err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
        Some("NoSuchKey".to_string())
    );

    let err = {
        let res = client.get_bucket_acl().bucket("no-such-bucket").send().await;
        res.err()
    };
    assert_eq!(
        err.map(|e| e.code().map(|c| c.to_owned())).flatten(),
        Some("NoSuchBucket".to_string())
    );

    Ok(())
}
