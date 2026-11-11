# tele-s3

S3-compatible gateway backed by Telegram — store S3 objects as Telegram messages.

Built with [`s3s`](https://github.com/Nugine/s3s) (S3 service), [`grammers`](https://github.com/Lonami/grammers) (Telegram client), [`redb`](https://github.com/cberner/redb) (embedded metadata store), and `tokio`/`hyper`.

> **Telegram as S3:** Each `PutObject`/`UploadPart` is written to Telegram via the `grammers` MTProto client (bot-token login, objects stored as message media); metadata (buckets, objects, versions, multipart state, blob ref-counts) lives in one redb file. The `Memory` backend is available for tests and local dev.

> **Clean cut:** This release replaces SQLite and SeaORM with redb. An existing `storage.db` file is ignored. The service does not read or import SQLite data.

## Features

- **S3 API** — `CreateBucket`/`DeleteBucket`/`HeadBucket`/`ListBuckets`/`GetBucketLocation`/`GetBucketVersioning`/`PutBucketVersioning`
- **Objects** — `PutObject`/`GetObject`/`HeadObject`/`DeleteObject`/`DeleteObjects`/`CopyObject`/`ListObjects`/`ListObjectsV2`/`ListObjectVersions`
- **Multipart** — `CreateMultipartUpload`/`UploadPart`/`UploadPartCopy`/`CompleteMultipartUpload`/`AbortMultipartUpload`/`ListParts`/`ListMultipartUploads` (with `key-marker`/`upload-id-marker` pagination)
- **Versioning** — `Enabled` / `Suspended` per bucket, `versionId` on every `Put`/`Copy`/`Complete`, delete markers, `ListObjectVersions`, `GET ?versionId`
- **Presigned URLs** — Standard AWS SigV4 `X-Amz-Signature` query auth (`s3s` `v4_check_presigned_url`); generate client-side and share publicly, no custom `/share` endpoint required
- **Buckets** — DNS-compliant name validation (`InvalidBucketName`), idempotent recreate of an owned bucket (200, no duplicate row), `DeleteBucket` rejected with `BucketNotEmpty` while multipart uploads are in flight
- **Other** — conditional reads (`If-Match`/`If-None-Match`/`If-Modified-Since`/`If-Unmodified-Since` with `412`/`304`), conditional writes (`PutObject` `If-Match`/`If-None-Match` incl. `*`, `CompleteMultipartUpload` `If-Match`, transactional CAS so concurrent versioned puts leave a single latest), `Content-MD5` validation, checksums (`crc32`/`crc32c`/`sha1`/`sha256` verified, `BadDigest`/`InvalidRequest` on mismatch, echoed on `Put`/`Get`/`Head`/`Copy`/`UploadPart`), user metadata (`x-amz-meta-*`), tagging (`Get/Put/DeleteObjectTagging`, plus `x-amz-tagging` header preserved on `Put`/`Copy`/`CreateMultipartUpload`/`Complete` with `TaggingDirective COPY`/`REPLACE`), delete-marker headers (`x-amz-delete-marker: true` on `GET`/`HEAD` of a marker), backend error mapping (`EntityTooLarge` on over-capacity writes, `InvalidRange` on out-of-range reads), ACL stubs, blob ref-counting for shared slices

## Quick Start

### Prerequisites

- Rust stable (see `mise.toml`; CI uses `cargo test --all-targets --all-features --locked`)
- Telegram `api_id`, `api_hash`, `bot_token` from https://my.telegram.org, plus a `username` destination (chat/channel) where objects will be stored

### Configure

```bash
cp config.example.toml config.toml
# edit config.toml
```

```toml
api_id = 1234567
api_hash = "344583e45741c457fe1862106095a5eb"
bot_token = "1234567890:AAHfiqksKZ8WmR2zSjiQ7_v4TMAKdiHm9T0"
# Destination chat/channel/peer objects are sent to (resolved via `resolve_username`, `send_message` to this peer).
username = "your_storage_channel_username"
database_path = "storage.redb"
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

Prebuilt multi-arch images are published to `ghcr.io/azamaulanaaa/tele-s3` on each release (see `.github/workflows/release-container-image.yaml`, tags `<version>` and `latest`).

Local build:

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
// -> http://localhost:8000/my-bucket/hello.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Expires=3600&X-Amz-Signature=...
// (path-style; the `localhost:9000` + `SingleDomain` virtual-host form
// `http://my-bucket.localhost:9000/...` is test-harness only in `tests/config.rs`.)
// Anyone with the URL can GET without Authorization until expiry.
// For versioned objects, `versionId` is automatically signed:
// client.get_object().bucket(b).key(k).version_id(vid).presigned(cfg).await?;
// -> .../hello.txt?versionId=abc-...&X-Amz-Algorithm=...
```

**AWS CLI:**

```bash
aws --endpoint-url $AWS_ENDPOINT_URL s3 presign s3://my-bucket/hello.txt --expires-in 3600
# share the returned URL
curl "http://localhost:8000/my-bucket/hello.txt?X-Amz-Algorithm=..."
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

Every setting comes from one of three sources. The resolver checks them in this order:

1. CLI flag.
2. Environment variable.
3. TOML value.
4. Built-in default.

A required setting must come from a source. Start-up fails when no source gives it. The error names the setting, its CLI flag, its environment variable, and its TOML key.

`-c` or `--config` gives the TOML path. `TELE_S3_CONFIG` is the fallback. If neither source gives a path, the code skips the TOML layer. The code does not search for `config.toml`.

| Setting | TOML key | CLI flag | Environment variable | Default |
|---|---|---|---|---|
| Telegram API id | `api_id` | `--api-id` | `TELE_S3_API_ID` | required |
| Telegram API hash | `api_hash` | `--api-hash` | `TELE_S3_API_HASH` | required |
| Bot token | `bot_token` | `--bot-token` | `TELE_S3_BOT_TOKEN` | required |
| Storage chat username | `username` | `--username` | `TELE_S3_USERNAME` | required |
| redb metadata path | `database_path` | `--database-path` | `TELE_S3_DATABASE_PATH` | required |
| Listen port | `listen_port` | `--listen-port` | `TELE_S3_LISTEN_PORT` | required |
| S3 access key | `auth_access_key` | `--auth-access-key` | `TELE_S3_AUTH_ACCESS_KEY` | required |
| S3 secret key | `auth_secret_key` | `--auth-secret-key` | `TELE_S3_AUTH_SECRET_KEY` | required |
| Max inbound connections | `max_concurrent_connections` | `--max-concurrent-connections` | `TELE_S3_MAX_CONCURRENT_CONNECTIONS` | `128` |
| Telegram max in-flight requests | `telegram_max_concurrent_requests` | `--telegram-max-concurrent-requests` | `TELEGRAM_MAX_CONCURRENT_REQUESTS` | `8` |
| Telegram I/O timeout seconds | `telegram_io_timeout_secs` | `--telegram-io-timeout-secs` | `TELEGRAM_IO_TIMEOUT_SECS` | `120` |
| Telegram max attempts | `telegram_max_attempts` | `--telegram-max-attempts` | `TELEGRAM_MAX_ATTEMPTS` | `8` |
| Log format | `log_format` | `--log-format` | `TELE_S3_LOG_FORMAT` | `text` |
| Log filter | `log_filter` | `--log-filter` | `RUST_LOG` | `tele_s3=info` |
| OTLP endpoint | `otlp_endpoint` | `--otlp-endpoint` | `TELE_S3_OTLP_ENDPOINT` | unset, export off |
| OTLP sample ratio | `otlp_sample_ratio` | `--otlp-sample-ratio` | `TELE_S3_OTLP_SAMPLE_RATIO` | `1.0` |

A bad environment value is not fatal. The code logs a warning. The warning names the variable and falls through to the next source. A bad CLI value is a usage error. A bad TOML value stops start-up. A blank environment value counts as unset.

`log_format` accepts `text` and `json`. The parser ignores case and surrounding whitespace.

The code clamps `max_concurrent_connections` to `1..=Semaphore::MAX_PERMITS`. The code clamps the Telegram limits with `GrammersLimits::sanitized()`. The introspection endpoint publishes the effective values.

`otlp_sample_ratio` must be finite and in `0.0..=1.0`. A blank `otlp_endpoint` means unset at every source.

### Environment aliases

The canonical names win when both names are set.

| Setting | Canonical variable | Alias |
|---|---|---|
| Telegram max in-flight requests | `TELEGRAM_MAX_CONCURRENT_REQUESTS` | `TELE_S3_MAX_CONCURRENT_REQUESTS` |
| Telegram I/O timeout seconds | `TELEGRAM_IO_TIMEOUT_SECS` | `TELE_S3_IO_TIMEOUT_SECS` |
| Telegram max attempts | `TELEGRAM_MAX_ATTEMPTS` | `TELE_S3_MAX_ATTEMPTS` |
| OTLP endpoint | `TELE_S3_OTLP_ENDPOINT` | `OTEL_EXPORTER_OTLP_ENDPOINT` |

### Source examples

A TOML file with every optional setting is in `config.example.toml`.

CLI only:

```bash
cargo run -- \
  --api-id 1234567 \
  --api-hash 344583e45741c457fe1862106095a5eb \
  --bot-token '1234567890:AAHfiqksKZ8WmR2zSjiQ7_v4TMAKdiHm9T0' \
  --username your_username \
  --database-path storage.redb \
  --listen-port 8000 \
  --auth-access-key YOUR_ACCESS_KEY \
  --auth-secret-key YOUR_SECRET_KEY
```

Environment only:

```bash
export TELE_S3_API_ID=1234567
export TELE_S3_API_HASH=344583e45741c457fe1862106095a5eb
export TELE_S3_BOT_TOKEN='1234567890:AAHfiqksKZ8WmR2zSjiQ7_v4TMAKdiHm9T0'
export TELE_S3_USERNAME=your_username
export TELE_S3_DATABASE_PATH=storage.redb
export TELE_S3_LISTEN_PORT=8000
export TELE_S3_AUTH_ACCESS_KEY=YOUR_ACCESS_KEY
export TELE_S3_AUTH_SECRET_KEY=YOUR_SECRET_KEY
cargo run
```

### Logging

The `log_filter` setting uses the usual `tracing-subscriber` filter syntax. `log_filter=tele_s3=debug` turns up the high-frequency internals: every uploaded part, every backend read, and every retry after a Telegram rate limit. Anything the dependencies emit needs their targets named explicitly, for example `log_filter=tele_s3=debug,s3s=info`.

`log_format=json` emits one JSON object per event. Use it for a log pipeline. Any other value from a CLI or TOML source stops start-up. Any other value from `TELE_S3_LOG_FORMAT` warns and falls through to the default `text`.

Every event this service emits carries its data as named fields rather than prose. The same event can be counted and grouped downstream: `bucket`, `key`, `blob_id`, `publish_id`, `message_id`, `part_index`, `bytes`, `attempts`, `duration_ms`, `timeout_ms`, `error.type`, `error.message` and so on. The full vocabulary is documented on `init_tracing` in `src/main.rs`. It is the contract a log or tracing exporter binds to.

```bash
TELE_S3_LOG_FORMAT=json RUST_LOG=tele_s3=debug cargo run -- --config config.toml
```

### Trace export (OTLP)

The same `tracing` spans that produce log lines are also exported as OTLP traces. A collector can show a request as a tree rather than as unrelated lines. Export is opt-in and off by default. With no endpoint, no exporter is built, no background thread starts, and the process runs as it did before.

| Setting | Value |
|---|---|
| Protocol | OTLP over HTTP with protobuf-encoded payloads. gRPC is not compiled in. |
| Export | Batching. A span costs a queue send. The exporter worker thread does the network work off the request path. |
| Resource attributes | `service.name=tele-s3` and `service.version` from the crate version. |
| Filtering | `log_filter` governs both the log layer and the trace layer. A `tele_s3=info` operator does not export the debug spans they asked not to see. |
| Shutdown | On `SIGINT` or `SIGTERM` the queue drains and the exporter shuts down before exit. A failed flush is logged with `error.type=telemetry_export` and ignored. |
| Failure | Telemetry never takes the server down. An unreachable collector or a bad endpoint is logged and ignored. |

Whether export is on is published at `/.tele-s3/introspect` as `telemetry.trace_export`. The value is a boolean. The endpoint itself is operator infrastructure and this document is unauthenticated.

```bash
TELE_S3_OTLP_ENDPOINT=http://collector:4318/v1/traces cargo run -- --config config.toml
```

### Admin endpoints

Three JSON endpoints share the S3 listener and are answered before any request reaches the S3 handlers. They are **unauthenticated** — a probe that has to sign a request is a probe that stops working when the signature expires — so they publish no credential material: no access key, secret key, `api_hash`, `bot_token` or `database_path`, and no error message that might quote one.

| Path | Answers |
|------|---------|
| `/.tele-s3/healthz` | Liveness: the process is alive and serving HTTP. Always `200`, derived from nothing — not Telegram, not the database. A rate-limited upstream must never make a healthy server look dead to its supervisor. |
| `/.tele-s3/readyz` | Readiness: start-up finished, so S3 traffic is being served. Always `200`, with the outcome of both start-up reconcilers in the body. |
| `/.tele-s3/introspect` | Introspection: the effective runtime state — the three Telegram knobs in force (after configuration resolution and clamping), the listen port, the version, whether OTLP trace export is on, and what each reconciler did. |

The `/.tele-s3` prefix is reserved deliberately: an S3 bucket name must begin and end with a lowercase letter or a digit, so `.tele-s3` can never be a bucket and these paths can never shadow `/{bucket}/{key}` (a bare `/healthz` would have been read as bucket `healthz`). Anything else under the prefix is a `404`.

Readiness does **not** fail when a start-up reconciler fails. Both passes are best-effort garbage collection: a failure leaves unreferenced rows that no listing shows and the next pass can still reclaim, while reads and writes are served either way. Reporting a failed pass as "not ready" would let an orchestrator stop routing traffic to a fully capable server, turning a garbage-collection nuisance into an outage. The outcome is reported in the readiness and introspection bodies, and logged at start-up.

```bash
curl -s localhost:8000/.tele-s3/healthz    # {"status":"ok"}
curl -s localhost:8000/.tele-s3/introspect # {"status":"ok","version":"0.7.1","listen_port":8000,...}
```

## Architecture

```
Client (aws-cli/sdk, curl) -> hyper -> S3ServiceBuilder{ TeleS3<Grammers>, SimpleAuth } -> TeleS3 (S3 trait)
                                                                                     |
                                                                                     v
                                                                           Repository (redb store)
                                                                     buckets / objects (versioned) / blobs (ref-count) / multipart uploads
                                                                                     |
                                                                                     v
                                                                               Backend (Grammers -> Telegram MTProto, Memory -> HashMap)
```

(`tests/config.rs` additionally sets `SingleDomain("localhost:9000")` for virtual-hosted-style test URLs; the production server in `src/main.rs` is path-style.)

Blob ref-counting keeps shared slices (e.g. `UploadPartCopy` ranges, `CopyObject`) alive until all referencing versions are deleted.

## Development

```bash
cargo test --all-targets --all-features --locked # 199 lib + 14 main + 36 aws + 2 conditional + 3 presigned (per CI in `.github/workflows/ci.yml`)
cargo test --test presigned -- --nocapture # presigned generation only (no endpoint)
```

`tests/config.rs` provides a `config::<N,M>()` helper that spins an in-memory `TeleS3<Memory<N,M>>` + `s3s_aws::Client` for integration tests — no real Telegram or network needed. Each test gets its own isolated in-memory redb store.

The store is synchronous. Repository methods remain `async` and run redb work through `tokio::task::spawn_blocking`. The code never holds a redb transaction across an `.await`.

## License

`LICENSE` (GPL-2.0 — the file in this repo is the GPLv2 text, and `Cargo.toml` uses `license-file = "LICENSE"`).
