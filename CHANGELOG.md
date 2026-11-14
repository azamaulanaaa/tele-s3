# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
