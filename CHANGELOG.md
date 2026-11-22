# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

- - -
## v0.9.0 - 2026-11-22
#### Features
- (**ci**) port the init Rust CI infrastructure - (e9d910b) - Aza Maulana
- (**release**) run the version bump in a smolvm clone - (1dcc61d) - Aza Maulana
- (**smolvm**) scope resources and cleanup to one checkout - (6f61ee3) - Aza Maulana
#### Bug Fixes
- (**ci**) restore the release gate on the ported checks workflow - (5fe050d) - Aza Maulana
- (**ci**) adapt the ported checks to this repository - (3154f3b) - Aza Maulana
- (**deps**) ignore unmaintained chrono advisory - (807757f) - Aza Maulana
- (**release**) trust the workspace git dir in the release clone - (0afeef9) - Aza Maulana
- (**release**) trust the guest workspace in the release clone - (e4b0dff) - Aza Maulana
- (**s3**) report content range for ranged get - (f20472d) - Aza Maulana
- (**smolvm**) exclude build and toolchain caches from the upload - (888cfb1) - Aza Maulana
- (**smolvm**) exclude the workspace temp dir from the upload - (cc95196) - Aza Maulana
#### Documentation
- (**changelog**) add the Cocogitto separator - (cd7f0f8) - Aza Maulana
- (**release**) document the sandbox release flow - (f978723) - Aza Maulana
- (**release**) pair cog with cargo-dist - (a208f21) - Aza Maulana
- (**smolvm**) reconcile the network and cleanup guidance - (0cd8a70) - Aza Maulana
- replace example secrets and fix editorconfig padding - (5c927ff) - Aza Maulana
#### Tests
- (**admin**) replace the AWS-shaped credential sentinel - (ef6f224) - Aza Maulana
#### Build system
- (**smolvm**) support remote owner - (d476af9) - Aza Maulana
#### Continuous Integration
- (**githook**) drop the redundant commit-msg hook - (0a9fc70) - Aza Maulana
- (**github**) adopt the init release workflow - (58c2ae8) - Aza Maulana
- (**release**) restore cargo-dist and container pipelines - (ab5b22f) - Aza Maulana
#### Refactoring
- <span style="background-color: #d73a49; color: white; padding: 2px 6px; border-radius: 3px; font-weight: bold; font-size: 0.85em;">BREAKING</span>(**smolvm**) rename SMOLVM_SERVE_ADDR to SMOLVM_ADDR - (9e07cb0) - Aza Maulana
#### Miscellaneous Chores
- (**reuse**) drop stale paths - (e86a542) - Aza Maulana
- (**smolvm**) ignore local smolvm cache and runtime state - (d739004) - Aza Maulana

- - -


## [0.8.0] - 2026-11-14

This is the first changelog entry. It covers the changes since 0.7.1.

### Changed

- **Breaking:** This release replaces the SQLite and SeaORM metadata store with redb. The S3 metadata and the Telegram session now live in one redb file. An existing `storage.db` file is ignored. The service does not read or import SQLite data.
- **Breaking:** The `database_path` setting replaces `database_uri`. The new setting names the redb file.
- A new layered resolver reads every setting from the CLI flag, the environment variable, the TOML file, and the built-in default, in that order. A bad environment value logs a warning and falls through to the next source. A bad CLI or TOML value stops start-up with an error that names the setting.
- The `--config` flag or `TELE_S3_CONFIG` selects the TOML file. The code does not search for `config.toml` when neither source gives a path.
- The container image runs as the non-root user `nonroot` (UID/GID 65532) from a pinned base image digest. The image uses `/data` as the working directory.

### Removed

- **Breaking:** The environment-variable aliases are removed. The old names no longer set a value. Use these names instead:
  - `TELEGRAM_MAX_CONCURRENT_REQUESTS` → `TELE_S3_MAX_CONCURRENT_REQUESTS`
  - `TELEGRAM_IO_TIMEOUT_SECS` → `TELE_S3_IO_TIMEOUT_SECS`
  - `TELEGRAM_MAX_ATTEMPTS` → `TELE_S3_MAX_ATTEMPTS`
  - `OTEL_EXPORTER_OTLP_ENDPOINT` → `TELE_S3_OTLP_ENDPOINT`

### Fixed

- Multipart completion now rejects a part below 5 MiB with `EntityTooSmall` when the part is not the last part. A single short part and a short last part remain valid.
