//! Configuration sources and resolution.
//!
//! One setting can come from a CLI flag, an environment variable, a TOML
//! value, or a built-in default. The resolver checks the sources in that order.

use std::{fmt::Display, fs::read_to_string, path::Path, time::Duration};

use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

use tele_s3::backend::GrammersLimits;

/// The error type for environment overrides that cannot be used.
const UNPARSABLE_ENV_ERROR_TYPE: &str = "unparsable_env_override";

/// Environment variable that gives the config file path.
const CONFIG_PATH_ENV: &str = "TELE_S3_CONFIG";

/// The default log filter.
const DEFAULT_LOG_FILTER: &str = "tele_s3=info";

/// The default trace sample ratio.
const DEFAULT_SAMPLE_RATIO: f64 = 1.0;

/// The log format of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogFormat {
    /// The human-readable text format.
    Text,
    /// One JSON object per event.
    Json,
}

impl LogFormat {
    /// Parse a format name.
    ///
    /// The parser ignores case and surrounding whitespace. It returns `None`
    /// for a name this build does not have.
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            _ => None,
        }
    }
}

/// One configurable setting and the names it has in each source.
#[derive(Debug)]
struct SettingSpec {
    /// The TOML key and the internal field name.
    name: &'static str,
    /// The CLI flag without the leading dashes.
    cli_flag: &'static str,
    /// The environment variable.
    env_var: &'static str,
    /// The setting has no built-in default.
    required: bool,
    /// The value must not appear in a log line.
    secret: bool,
}

static API_ID: SettingSpec = SettingSpec {
    name: "api_id",
    cli_flag: "api-id",
    env_var: "TELE_S3_API_ID",
    required: true,
    secret: false,
};
static API_HASH: SettingSpec = SettingSpec {
    name: "api_hash",
    cli_flag: "api-hash",
    env_var: "TELE_S3_API_HASH",
    required: true,
    secret: true,
};
static BOT_TOKEN: SettingSpec = SettingSpec {
    name: "bot_token",
    cli_flag: "bot-token",
    env_var: "TELE_S3_BOT_TOKEN",
    required: true,
    secret: true,
};
static USERNAME: SettingSpec = SettingSpec {
    name: "username",
    cli_flag: "username",
    env_var: "TELE_S3_USERNAME",
    required: true,
    secret: false,
};
static DATABASE_PATH: SettingSpec = SettingSpec {
    name: "database_path",
    cli_flag: "database-path",
    env_var: "TELE_S3_DATABASE_PATH",
    required: true,
    secret: false,
};
static LISTEN_PORT: SettingSpec = SettingSpec {
    name: "listen_port",
    cli_flag: "listen-port",
    env_var: "TELE_S3_LISTEN_PORT",
    required: true,
    secret: false,
};
static AUTH_ACCESS_KEY: SettingSpec = SettingSpec {
    name: "auth_access_key",
    cli_flag: "auth-access-key",
    env_var: "TELE_S3_AUTH_ACCESS_KEY",
    required: true,
    secret: true,
};
static AUTH_SECRET_KEY: SettingSpec = SettingSpec {
    name: "auth_secret_key",
    cli_flag: "auth-secret-key",
    env_var: "TELE_S3_AUTH_SECRET_KEY",
    required: true,
    secret: true,
};
static MAX_CONCURRENT_CONNECTIONS: SettingSpec = SettingSpec {
    name: "max_concurrent_connections",
    cli_flag: "max-concurrent-connections",
    env_var: "TELE_S3_MAX_CONCURRENT_CONNECTIONS",
    required: false,
    secret: false,
};
static TELEGRAM_MAX_CONCURRENT_REQUESTS: SettingSpec = SettingSpec {
    name: "telegram_max_concurrent_requests",
    cli_flag: "telegram-max-concurrent-requests",
    env_var: "TELE_S3_MAX_CONCURRENT_REQUESTS",
    required: false,
    secret: false,
};
static TELEGRAM_IO_TIMEOUT_SECS: SettingSpec = SettingSpec {
    name: "telegram_io_timeout_secs",
    cli_flag: "telegram-io-timeout-secs",
    env_var: "TELE_S3_IO_TIMEOUT_SECS",
    required: false,
    secret: false,
};
static TELEGRAM_MAX_ATTEMPTS: SettingSpec = SettingSpec {
    name: "telegram_max_attempts",
    cli_flag: "telegram-max-attempts",
    env_var: "TELE_S3_MAX_ATTEMPTS",
    required: false,
    secret: false,
};
static LOG_FORMAT: SettingSpec = SettingSpec {
    name: "log_format",
    cli_flag: "log-format",
    env_var: "TELE_S3_LOG_FORMAT",
    required: false,
    secret: false,
};
static LOG_FILTER: SettingSpec = SettingSpec {
    name: "log_filter",
    cli_flag: "log-filter",
    env_var: "RUST_LOG",
    required: false,
    secret: false,
};
static OTLP_ENDPOINT: SettingSpec = SettingSpec {
    name: "otlp_endpoint",
    cli_flag: "otlp-endpoint",
    env_var: "TELE_S3_OTLP_ENDPOINT",
    required: false,
    secret: false,
};
static OTLP_SAMPLE_RATIO: SettingSpec = SettingSpec {
    name: "otlp_sample_ratio",
    cli_flag: "otlp-sample-ratio",
    env_var: "TELE_S3_OTLP_SAMPLE_RATIO",
    required: false,
    secret: false,
};

/// Every setting in the configuration surface.
#[cfg(test)]
static SETTINGS: &[&SettingSpec] = &[
    &API_ID,
    &API_HASH,
    &BOT_TOKEN,
    &USERNAME,
    &DATABASE_PATH,
    &LISTEN_PORT,
    &AUTH_ACCESS_KEY,
    &AUTH_SECRET_KEY,
    &MAX_CONCURRENT_CONNECTIONS,
    &TELEGRAM_MAX_CONCURRENT_REQUESTS,
    &TELEGRAM_IO_TIMEOUT_SECS,
    &TELEGRAM_MAX_ATTEMPTS,
    &LOG_FORMAT,
    &LOG_FILTER,
    &OTLP_ENDPOINT,
    &OTLP_SAMPLE_RATIO,
];

/// The CLI layer.
#[derive(Debug, Clone, Parser)]
#[command(version, about)]
pub(crate) struct Args {
    /// Path to the TOML config file.
    #[arg(short, long)]
    pub config: Option<String>,

    /// Telegram API id.
    #[arg(long)]
    pub api_id: Option<i32>,

    /// Telegram API hash.
    #[arg(long)]
    pub api_hash: Option<String>,

    /// Telegram bot token.
    #[arg(long)]
    pub bot_token: Option<String>,

    /// Chat or channel that stores the objects.
    #[arg(long)]
    pub username: Option<String>,

    /// Path to the redb metadata file.
    #[arg(long)]
    pub database_path: Option<String>,

    /// HTTP listen port.
    #[arg(long)]
    pub listen_port: Option<u16>,

    /// S3 access key.
    #[arg(long)]
    pub auth_access_key: Option<String>,

    /// S3 secret key.
    #[arg(long)]
    pub auth_secret_key: Option<String>,

    /// Maximum number of inbound S3 connections served at once.
    #[arg(long)]
    pub max_concurrent_connections: Option<usize>,

    /// Maximum number of outbound Telegram requests in flight at once.
    #[arg(long)]
    pub telegram_max_concurrent_requests: Option<usize>,

    /// Timeout for one outbound Telegram request, in seconds.
    #[arg(long)]
    pub telegram_io_timeout_secs: Option<u64>,

    /// Maximum attempts per Telegram retry loop.
    #[arg(long)]
    pub telegram_max_attempts: Option<u32>,

    /// Log format. Accepts `text` or `json`.
    #[arg(long)]
    pub log_format: Option<String>,

    /// Log filter. Uses the `tracing-subscriber` syntax.
    #[arg(long)]
    pub log_filter: Option<String>,

    /// OTLP trace endpoint. An unset value turns export off.
    #[arg(long)]
    pub otlp_endpoint: Option<String>,

    /// Fraction of traces to export. Accepts `0.0` to `1.0`.
    #[arg(long)]
    pub otlp_sample_ratio: Option<f64>,
}

/// The TOML layer.
///
/// Every field is optional. A missing key leaves the decision to a lower
/// source.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileConfig {
    pub api_id: Option<i32>,
    pub api_hash: Option<String>,
    pub bot_token: Option<String>,
    pub username: Option<String>,
    pub database_path: Option<String>,
    pub listen_port: Option<u16>,
    pub auth_access_key: Option<String>,
    pub auth_secret_key: Option<String>,
    pub max_concurrent_connections: Option<usize>,
    pub telegram_max_concurrent_requests: Option<usize>,
    pub telegram_io_timeout_secs: Option<u64>,
    pub telegram_max_attempts: Option<u32>,
    pub log_format: Option<String>,
    pub log_filter: Option<String>,
    pub otlp_endpoint: Option<String>,
    pub otlp_sample_ratio: Option<f64>,
}

/// A non-fatal problem in one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolutionWarning {
    env_var: &'static str,
    value: Option<String>,
    message: String,
}

impl ResolutionWarning {
    /// Write the warning to the process log.
    pub(crate) fn emit(&self) {
        match &self.value {
            Some(value) => tracing::warn!(
                env_var = self.env_var,
                value = %value,
                error.type = UNPARSABLE_ENV_ERROR_TYPE,
                "{}",
                self.message
            ),
            None => tracing::warn!(
                env_var = self.env_var,
                error.type = UNPARSABLE_ENV_ERROR_TYPE,
                "{}",
                self.message
            ),
        }
    }
}

/// The resolved settings and the warnings from resolution.
#[derive(Debug)]
pub(crate) struct ResolvedConfig {
    /// The settings that the process uses.
    pub config: Config,
    /// Non-fatal source problems. The caller logs these after it installs
    /// tracing.
    pub warnings: Vec<ResolutionWarning>,
}

/// The resolved configuration.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Config {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
    pub username: String,

    /// Path to the redb metadata file, for example `"storage.redb"`.
    pub database_path: String,

    pub listen_port: u16,
    pub auth_access_key: String,
    pub auth_secret_key: String,

    /// Maximum number of inbound S3 connections served at once.
    ///
    /// An excess connection receives `503 Service Unavailable` rather than
    /// queueing, so a single slow client cannot exhaust the listener.
    pub max_concurrent_connections: usize,

    /// Maximum number of outbound Telegram requests in flight at once.
    pub telegram_max_concurrent_requests: usize,
    /// Timeout for one outbound Telegram request, in seconds.
    pub telegram_io_timeout_secs: u64,
    /// Maximum attempts per Telegram retry loop.
    pub telegram_max_attempts: u32,

    /// The process log format.
    pub log_format: LogFormat,
    /// The `tracing-subscriber` filter.
    pub log_filter: String,
    /// The OTLP endpoint. `None` turns trace and metric export off.
    pub otlp_endpoint: Option<String>,
    /// Fraction of traces to export.
    pub otlp_sample_ratio: f64,
}

/// An environment lookup. `None` means that the variable is unset.
pub(crate) type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// Read one variable from the process environment.
///
/// A non-Unicode value counts as unset.
pub(crate) fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

impl Config {
    /// Resolve every setting from the CLI, the environment, TOML, and the
    /// defaults.
    pub(crate) fn resolve(args: Args, env: EnvLookup<'_>) -> Result<ResolvedConfig, ConfigError> {
        let file = match config_path(&args, env) {
            Some(path) => FileConfig::from_path(Path::new(&path))?,
            None => FileConfig::default(),
        };

        Self::resolve_layers(args, env, file)
    }

    /// Sanitize the Telegram limits once.
    ///
    /// The caller passes the result to both the backend and the admin state.
    pub(crate) fn telegram_limits(&self) -> GrammersLimits {
        GrammersLimits {
            max_concurrent_requests: self.telegram_max_concurrent_requests,
            io_timeout: Duration::from_secs(self.telegram_io_timeout_secs),
            max_attempts: self.telegram_max_attempts,
        }
        .sanitized()
    }

    /// Resolve from explicit layers. This function does not read the process
    /// environment or the filesystem.
    fn resolve_layers(
        args: Args,
        env: EnvLookup<'_>,
        file: FileConfig,
    ) -> Result<ResolvedConfig, ConfigError> {
        let mut warnings = Vec::new();
        let defaults = GrammersLimits::default();

        let api_id = resolve_field(
            &API_ID,
            FieldLayers::new(args.api_id, file.api_id, None),
            env,
            parse_number::<i32>,
            &mut warnings,
        )?;
        let api_hash = resolve_field(
            &API_HASH,
            FieldLayers::new(args.api_hash, file.api_hash, None),
            env,
            parse_text,
            &mut warnings,
        )?;
        let bot_token = resolve_field(
            &BOT_TOKEN,
            FieldLayers::new(args.bot_token, file.bot_token, None),
            env,
            parse_text,
            &mut warnings,
        )?;
        let username = resolve_field(
            &USERNAME,
            FieldLayers::new(args.username, file.username, None),
            env,
            parse_text,
            &mut warnings,
        )?;
        let database_path = resolve_field(
            &DATABASE_PATH,
            FieldLayers::new(args.database_path, file.database_path, None),
            env,
            parse_text,
            &mut warnings,
        )?;
        let listen_port = resolve_field(
            &LISTEN_PORT,
            FieldLayers::new(args.listen_port, file.listen_port, None),
            env,
            parse_number::<u16>,
            &mut warnings,
        )?;
        let auth_access_key = resolve_field(
            &AUTH_ACCESS_KEY,
            FieldLayers::new(args.auth_access_key, file.auth_access_key, None),
            env,
            parse_text,
            &mut warnings,
        )?;
        let auth_secret_key = resolve_field(
            &AUTH_SECRET_KEY,
            FieldLayers::new(args.auth_secret_key, file.auth_secret_key, None),
            env,
            parse_text,
            &mut warnings,
        )?;

        let max_concurrent_connections = resolve_field(
            &MAX_CONCURRENT_CONNECTIONS,
            FieldLayers::new(
                args.max_concurrent_connections,
                file.max_concurrent_connections,
                Some(128),
            ),
            env,
            parse_number::<usize>,
            &mut warnings,
        )?
        .clamp(1, Semaphore::MAX_PERMITS);

        let telegram_max_concurrent_requests = resolve_field(
            &TELEGRAM_MAX_CONCURRENT_REQUESTS,
            FieldLayers::new(
                args.telegram_max_concurrent_requests,
                file.telegram_max_concurrent_requests,
                Some(defaults.max_concurrent_requests),
            ),
            env,
            parse_number::<usize>,
            &mut warnings,
        )?;
        let telegram_io_timeout_secs = resolve_field(
            &TELEGRAM_IO_TIMEOUT_SECS,
            FieldLayers::new(
                args.telegram_io_timeout_secs,
                file.telegram_io_timeout_secs,
                Some(defaults.io_timeout.as_secs()),
            ),
            env,
            parse_number::<u64>,
            &mut warnings,
        )?;
        let telegram_max_attempts = resolve_field(
            &TELEGRAM_MAX_ATTEMPTS,
            FieldLayers::new(
                args.telegram_max_attempts,
                file.telegram_max_attempts,
                Some(defaults.max_attempts),
            ),
            env,
            parse_number::<u32>,
            &mut warnings,
        )?;

        let cli_log_format = args
            .log_format
            .as_deref()
            .map(|raw| {
                LogFormat::parse(raw)
                    .ok_or_else(|| invalid_value(&LOG_FORMAT, "the --log-format flag", raw))
            })
            .transpose()?;
        let file_log_format = file
            .log_format
            .as_deref()
            .map(|raw| {
                LogFormat::parse(raw)
                    .ok_or_else(|| invalid_value(&LOG_FORMAT, "the log_format TOML key", raw))
            })
            .transpose()?;
        let log_format = resolve_field(
            &LOG_FORMAT,
            FieldLayers::new(cli_log_format, file_log_format, Some(LogFormat::Text)),
            env,
            parse_env_log_format,
            &mut warnings,
        )?;

        let log_filter = resolve_field(
            &LOG_FILTER,
            FieldLayers::new(
                args.log_filter,
                file.log_filter,
                Some(DEFAULT_LOG_FILTER.to_string()),
            ),
            env,
            parse_text,
            &mut warnings,
        )?;

        let otlp_endpoint = resolve_endpoint(
            args.otlp_endpoint.as_deref(),
            env_value(&OTLP_ENDPOINT, env).map(|(_, raw)| raw),
            file.otlp_endpoint.as_deref(),
        );

        let cli_sample_ratio = args
            .otlp_sample_ratio
            .map(|ratio| {
                validate_sample_ratio(&OTLP_SAMPLE_RATIO, "the --otlp-sample-ratio flag", ratio)
            })
            .transpose()?;
        let file_sample_ratio = file
            .otlp_sample_ratio
            .map(|ratio| {
                validate_sample_ratio(&OTLP_SAMPLE_RATIO, "the otlp_sample_ratio TOML key", ratio)
            })
            .transpose()?;
        let otlp_sample_ratio = resolve_field(
            &OTLP_SAMPLE_RATIO,
            FieldLayers::new(
                cli_sample_ratio,
                file_sample_ratio,
                Some(DEFAULT_SAMPLE_RATIO),
            ),
            env,
            parse_env_sample_ratio,
            &mut warnings,
        )?;

        Ok(ResolvedConfig {
            config: Self {
                api_id,
                api_hash,
                bot_token,
                username,
                database_path,
                listen_port,
                auth_access_key,
                auth_secret_key,
                max_concurrent_connections,
                telegram_max_concurrent_requests,
                telegram_io_timeout_secs,
                telegram_max_attempts,
                log_format,
                log_filter,
                otlp_endpoint,
                otlp_sample_ratio,
            },
            warnings,
        })
    }
}

/// A config path from the CLI, then from the environment.
fn config_path(args: &Args, env: EnvLookup<'_>) -> Option<String> {
    let cli = non_blank(args.config.as_deref());
    if cli.is_some() {
        return cli;
    }

    non_blank(env(CONFIG_PATH_ENV).as_deref())
}

/// Remove surrounding whitespace. A blank value becomes `None`.
fn non_blank(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Read the environment variable for one setting.
///
/// A blank value counts as unset.
fn env_value(spec: &'static SettingSpec, env: EnvLookup<'_>) -> Option<(&'static str, String)> {
    non_blank(env(spec.env_var).as_deref()).map(|raw| (spec.env_var, raw))
}

/// The values one setting has in the strict sources and the default.
struct FieldLayers<T> {
    cli: Option<T>,
    toml: Option<T>,
    default: Option<T>,
}

impl<T> FieldLayers<T> {
    /// Group the per-source values for one setting.
    fn new(cli: Option<T>, toml: Option<T>, default: Option<T>) -> Self {
        Self { cli, toml, default }
    }
}

/// Resolve one setting from the CLI, the environment, TOML, and the default,
/// in that order.
fn resolve_field<T>(
    spec: &'static SettingSpec,
    layers: FieldLayers<T>,
    env: EnvLookup<'_>,
    parse_env: fn(&str) -> Result<T, String>,
    warnings: &mut Vec<ResolutionWarning>,
) -> Result<T, ConfigError> {
    if let Some(value) = layers.cli {
        return Ok(value);
    }

    if let Some((env_var, raw)) = env_value(spec, env) {
        match parse_env(&raw) {
            Ok(value) => return Ok(value),
            Err(reason) => warnings.push(ResolutionWarning {
                env_var,
                value: (!spec.secret).then_some(raw),
                message: if spec.secret {
                    "ignoring unparsable environment override".to_string()
                } else {
                    format!("ignoring unparsable environment override: {reason}")
                },
            }),
        }
    }

    if let Some(value) = layers.toml {
        return Ok(value);
    }

    if let Some(value) = layers.default {
        return Ok(value);
    }

    if spec.required {
        Err(ConfigError::missing(spec))
    } else {
        Err(ConfigError::invalid(
            spec,
            "every source",
            "the setting is optional but has no default",
        ))
    }
}

/// Pick the first non-blank endpoint from the CLI, the environment, and TOML.
fn resolve_endpoint(cli: Option<&str>, env: Option<String>, toml: Option<&str>) -> Option<String> {
    non_blank(cli)
        .or_else(|| non_blank(env.as_deref()))
        .or_else(|| non_blank(toml))
}

/// Parse a number from an environment value.
fn parse_number<T>(raw: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: Display,
{
    raw.parse::<T>().map_err(|err| err.to_string())
}

/// Accept a non-blank string from the environment.
fn parse_text(raw: &str) -> Result<String, String> {
    Ok(raw.to_string())
}

/// Parse a log format from an environment value.
fn parse_env_log_format(raw: &str) -> Result<LogFormat, String> {
    LogFormat::parse(raw).ok_or_else(|| unknown_log_format(raw))
}

/// Parse a sample ratio from an environment value.
fn parse_env_sample_ratio(raw: &str) -> Result<f64, String> {
    let ratio = raw.parse::<f64>().map_err(|err| err.to_string())?;
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err("the ratio is not a finite number in 0.0..=1.0".to_string())
    }
}

/// Check a sample ratio from a strict source.
fn validate_sample_ratio(
    spec: &'static SettingSpec,
    source: &'static str,
    ratio: f64,
) -> Result<f64, ConfigError> {
    if ratio.is_finite() && (0.0..=1.0).contains(&ratio) {
        Ok(ratio)
    } else {
        Err(ConfigError::invalid(
            spec,
            source,
            "the ratio is not a finite number in 0.0..=1.0",
        ))
    }
}

/// Build the message for an unknown log format.
fn unknown_log_format(raw: &str) -> String {
    format!("unknown log format {raw:?}. Use \"text\" or \"json\".")
}

/// Build the error for an invalid value from a strict source.
fn invalid_value(spec: &'static SettingSpec, source: &'static str, raw: &str) -> ConfigError {
    ConfigError::invalid(spec, source, unknown_log_format(raw))
}

/// An error during configuration loading or resolution.
#[derive(Debug, thiserror::Error)]
pub(crate) struct ConfigError {
    kind: ConfigErrorKind,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

#[derive(Debug, thiserror::Error)]
enum ConfigErrorKind {
    #[error("Invalid config file: {0}")]
    InvalidFile(&'static str),
    #[error(
        "Missing required setting `{name}`. Set `--{cli_flag}`, `{env_var}`, or `{name}` in the config file."
    )]
    MissingRequired {
        name: &'static str,
        cli_flag: &'static str,
        env_var: &'static str,
    },
    #[error("Invalid value for `{name}` from {origin}: {reason}")]
    InvalidValue {
        name: &'static str,
        origin: &'static str,
        reason: String,
    },
}

impl ConfigError {
    /// A required setting has no value.
    fn missing(spec: &'static SettingSpec) -> Self {
        Self {
            kind: ConfigErrorKind::MissingRequired {
                name: spec.name,
                cli_flag: spec.cli_flag,
                env_var: spec.env_var,
            },
            source: None,
        }
    }

    /// A value from a strict source is not valid.
    fn invalid(
        spec: &'static SettingSpec,
        origin: &'static str,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            kind: ConfigErrorKind::InvalidValue {
                name: spec.name,
                origin,
                reason: reason.into(),
            },
            source: None,
        }
    }

    /// A file cannot be read or parsed.
    fn invalid_file(
        message: &'static str,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    ) -> Self {
        Self {
            kind: ConfigErrorKind::InvalidFile(message),
            source,
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

impl FileConfig {
    /// Read and parse a TOML file.
    fn from_path(path: &Path) -> Result<Self, ConfigError> {
        if !path.is_file() {
            return Err(ConfigError::invalid_file(
                "the path does not lead to a file",
                None,
            ));
        }

        let content = read_to_string(path).map_err(|err| {
            ConfigError::invalid_file("the file cannot be read", Some(Box::new(err)))
        })?;

        Self::parse(&content)
    }

    /// Parse TOML text.
    fn parse(content: &str) -> Result<Self, ConfigError> {
        toml::from_str(content).map_err(|err| {
            ConfigError::invalid_file("the TOML content is not valid", Some(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used, missing_docs, dead_code)]

    use std::collections::{HashMap, HashSet};

    use clap::CommandFactory;

    use super::*;

    fn empty_args() -> Args {
        Args::parse_from(["tele-s3"])
    }

    fn env_map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    fn resolve_with(
        args: Args,
        env: &HashMap<String, String>,
        file: FileConfig,
    ) -> Result<ResolvedConfig, ConfigError> {
        Config::resolve_layers(args, &|name| env.get(name).cloned(), file)
    }

    fn required_file() -> FileConfig {
        FileConfig {
            api_id: Some(12345),
            api_hash: Some("api-hash".to_string()),
            bot_token: Some("12345:bot-token".to_string()),
            username: Some("storage-channel".to_string()),
            database_path: Some("storage.redb".to_string()),
            listen_port: Some(8000),
            auth_access_key: Some("access-key".to_string()),
            auth_secret_key: Some("secret-key".to_string()),
            ..FileConfig::default()
        }
    }

    fn complete_file() -> FileConfig {
        FileConfig {
            max_concurrent_connections: Some(128),
            telegram_max_concurrent_requests: Some(8),
            telegram_io_timeout_secs: Some(120),
            telegram_max_attempts: Some(8),
            log_format: Some("text".to_string()),
            log_filter: Some("tele_s3=info".to_string()),
            otlp_endpoint: None,
            otlp_sample_ratio: Some(1.0),
            ..required_file()
        }
    }

    fn sample_toml(name: &str) -> &'static str {
        match name {
            "api_id" => "12345",
            "api_hash" => "\"api-hash\"",
            "bot_token" => "\"12345:bot-token\"",
            "username" => "\"storage-channel\"",
            "database_path" => "\"storage.redb\"",
            "listen_port" => "8000",
            "auth_access_key" => "\"access-key\"",
            "auth_secret_key" => "\"secret-key\"",
            "max_concurrent_connections" => "128",
            "telegram_max_concurrent_requests" => "8",
            "telegram_io_timeout_secs" => "120",
            "telegram_max_attempts" => "8",
            "log_format" => "\"text\"",
            "log_filter" => "\"tele_s3=info\"",
            "otlp_endpoint" => "\"\"",
            "otlp_sample_ratio" => "1.0",
            _ => panic!("no sample value for setting {name}"),
        }
    }

    fn complete_toml() -> String {
        SETTINGS
            .iter()
            .map(|spec| format!("{} = {}\n", spec.name, sample_toml(spec.name)))
            .collect()
    }

    fn clear_required(file: &mut FileConfig, name: &str) {
        match name {
            "api_id" => file.api_id = None,
            "api_hash" => file.api_hash = None,
            "bot_token" => file.bot_token = None,
            "username" => file.username = None,
            "database_path" => file.database_path = None,
            "listen_port" => file.listen_port = None,
            "auth_access_key" => file.auth_access_key = None,
            "auth_secret_key" => file.auth_secret_key = None,
            _ => panic!("{name} is not a required setting"),
        }
    }

    #[test]
    fn cli_beats_env_and_toml() {
        let mut args = empty_args();
        args.listen_port = Some(7002);

        let file = FileConfig {
            listen_port: Some(7000),
            ..required_file()
        };
        let env = env_map(&[("TELE_S3_LISTEN_PORT", "7001")]);

        let resolved = resolve_with(args, &env, file).expect("the config resolves");
        assert_eq!(resolved.config.listen_port, 7002);
    }

    #[test]
    fn env_beats_toml_and_default() {
        let file = FileConfig {
            listen_port: Some(7000),
            ..required_file()
        };
        let env = env_map(&[("TELE_S3_LISTEN_PORT", "7001")]);

        let resolved = resolve_with(empty_args(), &env, file).expect("the config resolves");
        assert_eq!(resolved.config.listen_port, 7001);
    }

    #[test]
    fn toml_beats_the_default() {
        let file = FileConfig {
            listen_port: Some(7000),
            ..required_file()
        };

        let resolved =
            resolve_with(empty_args(), &HashMap::new(), file).expect("the config resolves");
        assert_eq!(resolved.config.listen_port, 7000);
    }

    #[test]
    fn optional_settings_use_their_documented_defaults() {
        let resolved = resolve_with(empty_args(), &HashMap::new(), required_file())
            .expect("the config resolves");
        let config = resolved.config;

        assert_eq!(config.max_concurrent_connections, 128);
        assert_eq!(config.telegram_max_concurrent_requests, 8);
        assert_eq!(config.telegram_io_timeout_secs, 120);
        assert_eq!(config.telegram_max_attempts, 8);
        assert_eq!(config.log_format, LogFormat::Text);
        assert_eq!(config.log_filter, "tele_s3=info");
        assert_eq!(config.otlp_endpoint, None);
        assert_eq!(config.otlp_sample_ratio, 1.0);
    }

    #[test]
    fn each_required_setting_names_all_three_sources_when_missing() {
        for spec in SETTINGS.iter().filter(|spec| spec.required) {
            let mut file = required_file();
            clear_required(&mut file, spec.name);

            let error = resolve_with(empty_args(), &HashMap::new(), file)
                .expect_err("a required setting is missing");
            let message = error.to_string();

            assert!(message.contains(spec.name), "{message}");
            assert!(message.contains(spec.cli_flag), "{message}");
            assert!(message.contains(spec.env_var), "{message}");
            assert!(message.contains("config file"), "{message}");
        }
    }

    #[test]
    fn an_invalid_environment_value_warns_and_falls_through_to_toml() {
        let file = FileConfig {
            listen_port: Some(7000),
            ..required_file()
        };
        let env = env_map(&[("TELE_S3_LISTEN_PORT", "not-a-port")]);

        let resolved = resolve_with(empty_args(), &env, file).expect("the config resolves");

        assert_eq!(resolved.config.listen_port, 7000);
        assert_eq!(resolved.warnings.len(), 1);
        assert_eq!(resolved.warnings[0].env_var, "TELE_S3_LISTEN_PORT");
        assert_eq!(resolved.warnings[0].value.as_deref(), Some("not-a-port"));
    }

    #[test]
    fn a_required_setting_resolves_from_the_cli_when_the_file_lacks_it() {
        let mut args = empty_args();
        args.listen_port = Some(7002);
        let mut file = required_file();
        file.listen_port = None;

        let resolved =
            resolve_with(args, &HashMap::new(), file).expect("the CLI supplies the required value");
        assert_eq!(resolved.config.listen_port, 7002);
    }

    #[test]
    fn a_required_setting_resolves_from_the_environment_when_the_file_lacks_it() {
        let env = env_map(&[("TELE_S3_LISTEN_PORT", "7001")]);
        let mut file = required_file();
        file.listen_port = None;

        let resolved =
            resolve_with(empty_args(), &env, file).expect("the environment supplies the value");
        assert_eq!(resolved.config.listen_port, 7001);
    }

    #[test]
    fn the_cli_config_path_beats_the_environment() {
        let mut args = empty_args();
        args.config = Some("cli.toml".to_string());
        let env = env_map(&[("TELE_S3_CONFIG", "env.toml")]);

        let path = config_path(&args, &|name| env.get(name).cloned());

        assert_eq!(path.as_deref(), Some("cli.toml"));
    }

    #[test]
    fn the_environment_gives_the_config_path_when_the_cli_does_not() {
        let env = env_map(&[("TELE_S3_CONFIG", "env.toml")]);

        let path = config_path(&empty_args(), &|name| env.get(name).cloned());

        assert_eq!(path.as_deref(), Some("env.toml"));
    }

    #[test]
    fn a_missing_config_file_is_an_error() {
        let path = std::env::temp_dir().join(format!(
            "tele-s3-missing-config-{}.toml",
            std::process::id()
        ));

        let error = FileConfig::from_path(&path).expect_err("the file does not exist");

        assert!(error.to_string().contains("does not lead to a file"));
    }

    #[test]
    fn an_unknown_toml_key_is_an_error() {
        let error = FileConfig::parse("listen_port = 8000\nunknown_setting = 1\n")
            .expect_err("an unknown key is an error");

        assert!(error.to_string().contains("Invalid config file"));
    }

    #[test]
    fn invalid_toml_is_an_error() {
        let error = FileConfig::parse("listen_port = \"not-a-port\"")
            .expect_err("a wrong TOML type is an error");
        assert!(error.to_string().contains("Invalid config file"));
    }

    #[test]
    fn the_tele_s3_telegram_names_resolve() {
        for (name, expected) in [
            ("TELE_S3_MAX_CONCURRENT_REQUESTS", "5"),
            ("TELE_S3_IO_TIMEOUT_SECS", "42"),
            ("TELE_S3_MAX_ATTEMPTS", "3"),
        ] {
            let env = env_map(&[(name, expected)]);
            let resolved =
                resolve_with(empty_args(), &env, required_file()).expect("the config resolves");

            let actual = match name {
                "TELE_S3_MAX_CONCURRENT_REQUESTS" => {
                    resolved.config.telegram_max_concurrent_requests.to_string()
                }
                "TELE_S3_IO_TIMEOUT_SECS" => resolved.config.telegram_io_timeout_secs.to_string(),
                _ => resolved.config.telegram_max_attempts.to_string(),
            };
            assert_eq!(actual, expected, "{name}");
        }
    }

    #[test]
    fn the_old_telegram_names_are_ignored() {
        let env = env_map(&[
            ("TELEGRAM_MAX_CONCURRENT_REQUESTS", "5"),
            ("TELEGRAM_IO_TIMEOUT_SECS", "42"),
            ("TELEGRAM_MAX_ATTEMPTS", "3"),
        ]);

        let resolved =
            resolve_with(empty_args(), &env, required_file()).expect("the config resolves");
        assert_eq!(resolved.config.telegram_max_concurrent_requests, 8);
        assert_eq!(resolved.config.telegram_io_timeout_secs, 120);
        assert_eq!(resolved.config.telegram_max_attempts, 8);
    }

    #[test]
    fn only_the_tele_s3_otlp_endpoint_is_read() {
        let standard = "http://otel-collector:4318/v1/traces";
        let own = "http://collector.internal:4318/v1/traces";

        let env = env_map(&[("OTEL_EXPORTER_OTLP_ENDPOINT", standard)]);
        let resolved =
            resolve_with(empty_args(), &env, required_file()).expect("the config resolves");
        assert_eq!(resolved.config.otlp_endpoint, None);

        let env = env_map(&[("TELE_S3_OTLP_ENDPOINT", own)]);
        let resolved =
            resolve_with(empty_args(), &env, required_file()).expect("the config resolves");
        assert_eq!(resolved.config.otlp_endpoint.as_deref(), Some(own));
    }

    #[test]
    fn every_setting_has_a_cli_flag_a_toml_key_and_an_env_var() {
        let command = Args::command();
        let flags: HashSet<&str> = command
            .get_arguments()
            .filter_map(|arg| arg.get_long())
            .collect();

        for spec in SETTINGS {
            assert!(flags.contains(spec.cli_flag), "{} has no flag", spec.name);

            let env_seen = std::cell::RefCell::new(Vec::new());
            let file = complete_file();
            let resolved = Config::resolve_layers(
                empty_args(),
                &|name| {
                    env_seen.borrow_mut().push(name.to_string());
                    None
                },
                file,
            )
            .expect("the complete file resolves");
            assert!(resolved.config.api_id > 0);
            assert!(
                env_seen.borrow().iter().any(|name| name == spec.env_var),
                "{} does not read {}",
                spec.name,
                spec.env_var
            );
        }

        let toml = complete_toml();
        let file = FileConfig::parse(&toml).expect("every table key maps to a TOML field");

        let serialized = serde_json::to_value(file).expect("the file config serializes");
        let toml_keys: HashSet<&str> = serialized
            .as_object()
            .expect("the file config is an object")
            .keys()
            .map(String::as_str)
            .collect();
        let table_keys: HashSet<&str> = SETTINGS.iter().map(|spec| spec.name).collect();
        assert_eq!(toml_keys, table_keys);

        let setting_flags: HashSet<&str> = SETTINGS.iter().map(|spec| spec.cli_flag).collect();
        let extra_flags: Vec<&str> = flags
            .iter()
            .copied()
            .filter(|flag| *flag != "config" && !setting_flags.contains(flag))
            .collect();
        assert_eq!(extra_flags, Vec::<&str>::new());
    }

    #[test]
    fn the_config_example_parses() {
        let content = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/config.example.toml"));
        FileConfig::parse(content).expect("the example config parses");
    }

    #[test]
    fn log_formats_accept_text_and_json() {
        for raw in ["text", "TEXT", " text \n"] {
            assert_eq!(LogFormat::parse(raw), Some(LogFormat::Text), "{raw}");
        }
        for raw in ["json", "JSON", " JSON \n"] {
            assert_eq!(LogFormat::parse(raw), Some(LogFormat::Json), "{raw}");
        }
    }

    #[test]
    fn an_unknown_log_format_is_rejected() {
        for raw in ["yaml", "1", ""] {
            assert_eq!(LogFormat::parse(raw), None, "{raw}");
        }
    }

    #[test]
    fn an_invalid_log_format_from_env_falls_through() {
        let env = env_map(&[("TELE_S3_LOG_FORMAT", "yaml")]);
        let resolved =
            resolve_with(empty_args(), &env, required_file()).expect("the config resolves");

        assert_eq!(resolved.config.log_format, LogFormat::Text);
        assert_eq!(resolved.warnings.len(), 1);
        assert_eq!(resolved.warnings[0].env_var, "TELE_S3_LOG_FORMAT");
    }

    #[test]
    fn an_invalid_log_format_from_cli_is_an_error() {
        let mut args = empty_args();
        args.log_format = Some("yaml".to_string());

        let error = resolve_with(args, &HashMap::new(), required_file())
            .expect_err("the CLI value is invalid");
        assert!(error.to_string().contains("--log-format"));
    }

    #[test]
    fn an_invalid_log_format_from_toml_is_an_error() {
        let file = FileConfig {
            log_format: Some("yaml".to_string()),
            ..required_file()
        };

        let error = resolve_with(empty_args(), &HashMap::new(), file)
            .expect_err("the TOML value is invalid");
        assert!(error.to_string().contains("log_format"));
    }

    #[test]
    fn an_invalid_sample_ratio_from_env_falls_through() {
        let env = env_map(&[("TELE_S3_OTLP_SAMPLE_RATIO", "1.5")]);
        let resolved =
            resolve_with(empty_args(), &env, required_file()).expect("the config resolves");

        assert_eq!(resolved.config.otlp_sample_ratio, 1.0);
        assert_eq!(resolved.warnings.len(), 1);
        assert_eq!(resolved.warnings[0].env_var, "TELE_S3_OTLP_SAMPLE_RATIO");
    }

    #[test]
    fn an_out_of_range_sample_ratio_from_cli_is_an_error() {
        let mut args = empty_args();
        args.otlp_sample_ratio = Some(f64::NAN);

        let error = resolve_with(args, &HashMap::new(), required_file())
            .expect_err("the CLI value is invalid");
        assert!(error.to_string().contains("--otlp-sample-ratio"));
    }

    #[test]
    fn an_out_of_range_sample_ratio_from_toml_is_an_error() {
        let file = FileConfig {
            otlp_sample_ratio: Some(-0.1),
            ..required_file()
        };

        let error = resolve_with(empty_args(), &HashMap::new(), file)
            .expect_err("the TOML value is invalid");
        assert!(error.to_string().contains("otlp_sample_ratio"));
    }

    #[test]
    fn a_blank_environment_value_counts_as_unset() {
        let env = env_map(&[("TELE_S3_LISTEN_PORT", "   ")]);
        let file = FileConfig {
            listen_port: Some(7000),
            ..required_file()
        };

        let resolved = resolve_with(empty_args(), &env, file).expect("the config resolves");
        assert_eq!(resolved.config.listen_port, 7000);
        assert!(resolved.warnings.is_empty());
    }

    #[test]
    fn a_blank_endpoint_is_unset_at_every_layer() {
        let file = FileConfig {
            otlp_endpoint: Some("   ".to_string()),
            ..required_file()
        };
        let env = env_map(&[("TELE_S3_OTLP_ENDPOINT", "  ")]);

        let resolved = resolve_with(empty_args(), &env, file).expect("the config resolves");
        assert_eq!(resolved.config.otlp_endpoint, None);
    }

    #[test]
    fn the_inbound_connection_limit_is_clamped() {
        let file = FileConfig {
            max_concurrent_connections: Some(usize::MAX),
            ..required_file()
        };

        let resolved =
            resolve_with(empty_args(), &HashMap::new(), file).expect("the config resolves");
        assert_eq!(
            resolved.config.max_concurrent_connections,
            Semaphore::MAX_PERMITS
        );

        let file = FileConfig {
            max_concurrent_connections: Some(0),
            ..required_file()
        };
        let resolved =
            resolve_with(empty_args(), &HashMap::new(), file).expect("the config resolves");
        assert_eq!(resolved.config.max_concurrent_connections, 1);
    }

    #[test]
    fn telegram_limits_are_sanitized_once() {
        let file = FileConfig {
            telegram_max_concurrent_requests: Some(0),
            telegram_io_timeout_secs: Some(0),
            telegram_max_attempts: Some(0),
            ..required_file()
        };

        let resolved =
            resolve_with(empty_args(), &HashMap::new(), file).expect("the config resolves");
        let limits = resolved.config.telegram_limits();

        assert_eq!(limits.max_concurrent_requests, 1);
        assert_eq!(limits.io_timeout, Duration::from_secs(1));
        assert_eq!(limits.max_attempts, 1);
    }
}
