# tele-s3

S3-compatible gateway backed by Telegram — store S3 objects as Telegram messages.

Built with [`s3s`](https://github.com/Nugine/s3s) (S3 service), [`grammers`](https://github.com/Lonami/grammers) (Telegram client), [`sea-orm`](https://github.com/SeaQL/sea-orm) (SQLite metadata), and `tokio`/`hyper`.

> **Telegram as S3:** Each `PutObject`/`UploadPart` is written to a Telegram channel via the bot API; metadata (buckets, objects, versions, multipart state, blob ref-counts) lives in SQLite. The `Memory` backend is available for tests and local dev.

## Features

- **S3 API** — `CreateBucket`/`DeleteBucket`/`HeadBucket`/`ListBuckets`/`GetBucketLocation`/`GetBucketVersioning`/`PutBucketVersioning`
- **Objects** — `PutObject`/`GetObject`/`HeadObject`/`DeleteObject`/`DeleteObjects`/`CopyObject`/`ListObjects`/`ListObjectsV2`/`ListObjectVersions`
- **Multipart** — `CreateMultipartUpload`/`UploadPart`/`UploadPartCopy`/`CompleteMultipartUpload`/`AbortMultipartUpload`/`ListParts`/`ListMultipartUploads`
- **Versioning** — `Enabled` / `Suspended` per bucket, `versionId` on every `Put`/`Copy`/`Complete`, delete markers, `ListObjectVersions`, `GET ?versionId`
- **Presigned URLs** — Standard AWS SigV4 `X-Amz-Signature` query auth (`s3s` `v4_check_presigned_url`); generate client-side and share publicly, no custom `/share` endpoint required
- **Other** — `If-Match`/`If-None-Match` (CAS), `Content-MD5` validation, checksums (`crc32`/`crc32c`/`sha1`/`sha256` echo), user metadata (`x-amz-meta-*`), tagging (`Get/Put/DeleteObjectTagging`), ACL stubs, blob ref-counting for shared slices

## Quick Start

### Prerequisites

- Rust stable (see `mise.toml`)
- Telegram `api_id`, `api_hash`, `bot_token` from https://my.telegram.org

### Configure

```bash
cp config.example.toml config.toml
# edit config.toml
```

```toml
api_id = 1234567
api_hash = "344583e45741c457fe1862106095a5eb"
bot_token = "1234567890:AAHfiqksKZ8WmR2zSjiQ7_v4TMAKdiHm9T0"
username = "your_bot_username"
database_uri = "sqlite://storage.db?mode=rwc"
listen_port = 8000
auth_access_key = "YOUR_ACCESS_KEY"
auth_secret_key = "YOUR_SECRET_KEY"
```

### Run

```bash
cargo run -- --config config.toml
# listening on 0.0.0.0:8000
```

### Docker

```bash
cargo build --profile dist
docker build -f .github/Dockerfile --build-arg BINARY_NAME=target/x86_64-unknown-linux-gnu/dist/tele-s3 -t tele-s3 .
docker run -p 8000:8000 -v ./config.toml:/config.toml tele-s3 --config /config.toml
```

## Usage

All examples use `awscurl` / `aws-cli` against `http://localhost:8000`. Set:

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_ENDPOINT_URL=http://localhost:8000
# aws-cli v2:
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls
```

### Bucket

```bash
aws --endpoint-url $AWS_ENDPOINT_URL s3 mb s3://my-bucket
aws --endpoint-url $AWS_ENDPOINT_URL s3api get-bucket-location --bucket my-bucket
aws --endpoint-url $AWS_ENDPOINT_URL s3api get-bucket-versioning --bucket my-bucket # {}
aws --endpoint-url $AWS_ENDPOINT_URL s3api put-bucket-versioning --bucket my-bucket --versioning-configuration Status=Enabled
```

### Objects

```bash
echo "hello" | aws --endpoint-url $AWS_ENDPOINT_URL s3 cp - s3://my-bucket/hello.txt
aws --endpoint-url $AWS_ENDPOINT_URL s3 cp s3://my-bucket/hello.txt -
aws --endpoint-url $AWS_ENDPOINT_URL s3 ls s3://my-bucket --recursive
```

### Presigned URLs (public share links)

No server endpoint needed — generate client-side with your credentials, share the URL. The server validates `X-Amz-Signature` via `s3s` `SimpleAuth`.

**Rust (`aws-sdk-s3`):**

```rust
use aws_sdk_s3::presigning::PresigningConfig;
use std::time::Duration;

let presigned = client.get_object()
    .bucket("my-bucket").key("hello.txt")
    .presigned(PresigningConfig::expires_in(Duration::from_secs(3600))?)
    .await?;
println!("{}", presigned.uri()); // share this
// -> http://my-bucket.localhost:9000/hello.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Expires=3600&X-Amz-Signature=...
// Anyone with the URL can GET without Authorization until expiry.
// For versioned objects, `versionId` is automatically signed:
// client.get_object().bucket(b).key(k).version_id(vid).presigned(cfg).await?;
// -> .../hello.txt?versionId=abc-...&X-Amz-Algorithm=...
```

**AWS CLI:**

```bash
aws --endpoint-url $AWS_ENDPOINT_URL s3 presign s3://my-bucket/hello.txt --expires-in 3600
# share the returned URL
curl "http://my-bucket.localhost:9000/hello.txt?X-Amz-Algorithm=..."
```

Tampered signatures and expired links are rejected with `403`. `HeadObject` can also be presigned.

### Versioning

```bash
aws --endpoint-url $AWS_ENDPOINT_URL s3api put-bucket-versioning --bucket my-bucket --versioning-configuration Status=Enabled
echo "v1" | aws --endpoint-url $AWS_ENDPOINT_URL s3 cp - s3://my-bucket/file.txt # -> versionId: abc
echo "v2" | aws --endpoint-url $AWS_ENDPOINT_URL s3 cp - s3://my-bucket/file.txt # -> versionId: def

aws --endpoint-url $AWS_ENDPOINT_URL s3api list-object-versions --bucket my-bucket --prefix file.txt
aws --endpoint-url $AWS_ENDPOINT_URL s3api get-object --bucket my-bucket --key file.txt --version-id abc /tmp/v1
aws --endpoint-url $AWS_ENDPOINT_URL s3api delete-object --bucket my-bucket --key file.txt # creates delete marker
aws --endpoint-url $AWS_ENDPOINT_URL s3api delete-object --bucket my-bucket --key file.txt --version-id abc # permanent delete
```

`suspended` mode stores new writes as `versionId=null` while retaining history. `ListObjects`/`ListObjectsV2` always show the current (non-delete-marker) version; `ListObjectVersions` shows all versions + delete markers.

## Configuration Reference

| Key | Description |
|-----|-------------|
| `api_id` / `api_hash` / `bot_token` / `username` | Telegram bot credentials |
| `database_uri` | SeaORM URI, e.g. `sqlite://storage.db?mode=rwc` or `sqlite::memory:` for tests |
| `listen_port` | HTTP listen port |
| `auth_access_key` / `auth_secret_key` | S3 `SimpleAuth` credentials (used for both header and presigned URL verification) |

## Architecture

```
Client (aws-cli/sdk, curl) -> hyper -> S3ServiceBuilder{ TeleS3<Grammers>, SimpleAuth, SingleDomain } -> TeleS3 (S3 trait)
                                                                                    |
                                                                                    v
                                                                          Repository (sea-orm, SQLite)
                                                                    s3_bucket / s3_object (versioned) / s3_blob (ref-count) / s3_multipart_upload_state
                                                                                    |
                                                                                    v
                                                                              Backend (Grammers -> Telegram, Memory -> HashMap)
```

Blob ref-counting keeps shared slices (e.g. `UploadPartCopy` ranges, `CopyObject`) alive until all referencing versions are deleted.

## Development

```bash
cargo test          # 11 lib + 28 aws + 3 presigned
cargo test --test presigned -- --nocapture # presigned generation only (no endpoint)
```

`tests/config.rs` provides `config::<N,M>()` helper that spins an in-memory `TeleS3<Memory<N,M>>` + `s3s_aws::Client` for integration tests — no real Telegram or network needed.

## License

`LICENSE` (MIT/Apache-2.0 as per original).
