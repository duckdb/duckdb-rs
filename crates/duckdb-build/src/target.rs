use std::{env, io};

use crate::LinkMode;

pub struct Target {
    pub os: String,
    pub environment: String,
}

impl Target {
    pub fn from_env() -> io::Result<Self> {
        let read = |name| {
            env::var(name).map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("duckdb-build must run from a Cargo build script ({name}): {error}"),
                )
            })
        };
        Ok(Self {
            os: read("CARGO_CFG_TARGET_OS")?,
            environment: read("CARGO_CFG_TARGET_ENV")?,
        })
    }

    pub fn library_names(&self, mode: LinkMode, name: &str) -> io::Result<Vec<String>> {
        let names = match (mode, self.os.as_str(), self.environment.as_str()) {
            (LinkMode::Static | LinkMode::Dynamic, "windows", "msvc") => {
                vec![format!("{name}.lib"), format!("lib{name}.lib")]
            }
            (LinkMode::Static, "linux" | "android" | "macos" | "ios", _)
            | (LinkMode::Static, "windows", "gnu" | "gnullvm") => {
                vec![format!("lib{name}.a")]
            }
            (LinkMode::Dynamic, "macos" | "ios", _) => vec![format!("lib{name}.dylib")],
            (LinkMode::Dynamic, "linux" | "android", _) => vec![format!("lib{name}.so")],
            (LinkMode::Dynamic, "windows", "gnu" | "gnullvm") => {
                vec![format!("lib{name}.dll.a"), format!("lib{name}.a")]
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("Unsupported DuckDB target: {}-{}", self.os, self.environment),
                ));
            }
        };
        Ok(names)
    }

    pub fn cxx_library(&self) -> Option<&'static str> {
        match self.os.as_str() {
            "macos" | "ios" => Some("c++"),
            "windows" if self.environment == "msvc" => None,
            _ => Some("stdc++"),
        }
    }
}
