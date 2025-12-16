use std::{fmt::Display, fs::read_to_string, path::Path};

use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum ConfigErrorKind {
    #[error("Invalid File: {0}")]
    InvalidFile(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub struct ConfigError {
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
pub struct Config {
    pub api_id: i32,
    pub api_hash: String,
    pub bot_token: String,
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
