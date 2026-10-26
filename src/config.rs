use std::{fmt::Display, fs::read_to_string, path::Path};

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConfigErrorKind {
    #[error("Invalid File: {0}")]
    InvalidFile(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub(crate) struct ConfigError {
    kind: ConfigErrorKind,
    #[source]
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.kind)
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Config {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
    pub username: String,

    pub database_uri: String,

    pub listen_port: u16,
    pub auth_access_key: String,
    pub auth_secret_key: String,

    /// Maximum number of inbound S3 connections served at once.
    ///
    /// An excess connection receives `503 Service Unavailable` rather than
    /// queueing, so a single slow client cannot exhaust the listener.
    #[serde(default = "default_max_concurrent_connections")]
    pub max_concurrent_connections: usize,
}

fn default_max_concurrent_connections() -> usize {
    128
}

impl TryFrom<&Path> for Config {
    type Error = ConfigError;

    fn try_from(value: &Path) -> Result<Self, Self::Error> {
        if !value.is_file() {
            return Err(ConfigError {
                kind: ConfigErrorKind::InvalidFile("The path does not lead to a file"),
                source: None,
            });
        }

        let content = read_to_string(value).map_err(|e| ConfigError {
            kind: ConfigErrorKind::InvalidFile("Unable to read string from file"),
            source: Some(Box::new(e)),
        })?;

        let config = toml::from_str::<Self>(&content).map_err(|e| ConfigError {
            kind: ConfigErrorKind::InvalidFile("Unable to parse the content to given format"),
            source: Some(Box::new(e)),
        })?;

        Ok(config)
    }
}
