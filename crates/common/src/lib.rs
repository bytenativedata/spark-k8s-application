pub mod utils;
pub mod logging;

use std::{path::{Path, PathBuf}, str::FromStr};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("A required file was not found from any of locations: [{search_path:?}]")]
    RequiredFileMissing {search_path: Vec<PathBuf>},

    #[error("File not found: {file_name}")]
    FileNotFound { file_name: PathBuf },

    #[error("Could not parse yaml file - {file}: {reason}")]
    YamlFileNotParsable { file: PathBuf, reason: String },

    #[error("Could not parse yaml - {content}: {reason}")]
    YamlNotParsable { content: String, reason: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WatchNamespace {
    All,
    One(String),
}

impl From<&str> for WatchNamespace {
    fn from(s: &str) -> Self {
        if s.is_empty() {
            WatchNamespace::All
        } else {
            WatchNamespace::One(s.to_string())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialOrd, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProductConfig {
    pub version: String,
    // TODO: more configs for product deply
}

impl FromStr for ProductConfig {
    type Err = Error;
    fn from_str(contents: &str) -> Result<Self> {
        serde_yaml::from_str(contents).map_err(|serde_error| {
            Error::YamlNotParsable {
                content: contents.to_string(),
                reason: serde_error.to_string(),
            }
        })
    }
}

impl ProductConfig {
    pub fn load_from(file_path: &Path) -> Result<Self, Error> {
        let contents = std::fs::read_to_string(&file_path).map_err(|_| Error::FileNotFound {
            file_name: file_path.to_path_buf(),
        })?;

        Self::from_str(&contents).map_err(|serde_error| Error::YamlFileNotParsable {
            file: file_path.to_path_buf(),
            reason: serde_error.to_string(),
        })
    }
}


/// A path to a [`ProductConfigManager`] spec file
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProductConfigPath {
    path: Option<PathBuf>,
}

impl From<&std::ffi::OsStr> for ProductConfigPath {
    fn from(s: &std::ffi::OsStr) -> Self {
        Self {
            // StructOpt doesn't let us hook in to see the underlying `Option<&str>`, so we treat the
            // otherwise-invalid `""` as a sentinel for using the default instead.
            path: if s.is_empty() { None } else { Some(s.into()) },
        }
    }
}

impl ProductConfigPath {
    /// Load the [`ProductConfigManager`] from the given path, falling back to the first
    /// path that exists from `default_search_paths` if none is given by the user.
    pub fn load(
        &self,
        default_search_paths: &[impl AsRef<std::path::Path>],
    ) -> Result<ProductConfig> {
        ProductConfig::load_from(resolve_path(
            self.path.as_deref(),
            default_search_paths,
        )?)
    }
}

/// Check if the path can be found anywhere:
/// 1) User provides path `user_provided_path` to file -> 'Error' if not existing.
/// 2) User does not provide path to file -> search in `default_paths` and
///    take the first existing file.
/// 3) `Error` if nothing was found.
fn resolve_path<'a>(
    user_provided_path: Option<&'a std::path::Path>,
    default_paths: &'a [impl AsRef<std::path::Path> + 'a],
) -> Result<&'a std::path::Path> {
    // Use override if specified by the user, otherwise search through defaults given
    let search_paths = if let Some(path) = user_provided_path {
        vec![path]
    } else {
        default_paths.iter().map(|path| path.as_ref()).collect()
    };
    for path in &search_paths {
        if path.exists() {
            return Ok(path);
        }
    }
    Err(Error::RequiredFileMissing {
        search_path: search_paths.into_iter().map(std::path::PathBuf::from).collect(),
    })
}