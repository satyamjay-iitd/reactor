use std::{fs, process::Command};
use tempfile::TempDir;

use cargo_toml::Manifest;
use libloading::Library;
use thiserror::Error;

pub struct LibBuilder {}

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Invalid Cargo.toml")]
    InvalidCargoToml,
    #[error("Couldn't Compile")]
    BuildFailed,
    #[error("Io")]
    Io(#[from] std::io::Error),
    #[error("Couldn't Load the Library")]
    LibraryLoadFailed,
    #[error("Code generation failed: {0}")]
    CodegenFailed(String),
}

impl LibBuilder {
    /// Build into an explicit directory instead of a randomly-named temp dir.
    /// The directory is created if it does not exist and persists after the call,
    /// so subsequent compilations reuse cargo’s incremental build cache.
    pub fn build_named(code: String, deps: String, build_dir: &std::path::Path) -> Result<Library, BuildError> {
        let cargo_manifest = Manifest::from_str(&deps).map_err(|_| BuildError::InvalidCargoToml)?;
        let crate_type = cargo_manifest
            .lib
            .as_ref()
            .ok_or(BuildError::InvalidCargoToml)?
            .crate_type
            .clone();
        crate_type
            .iter()
            .find(|ct| *ct == "cdylib")
            .ok_or(BuildError::InvalidCargoToml)?;
        let library_name = cargo_manifest
            .lib
            .unwrap()
            .name
            .ok_or(BuildError::InvalidCargoToml)?;

        fs::create_dir_all(build_dir.join("src"))?;
        fs::write(build_dir.join("Cargo.toml"), deps)?;
        fs::write(build_dir.join("src").join("lib.rs"), code)?;

        let status = Command::new("cargo")
            .args(["build", "--release"])
            .current_dir(build_dir)
            .status()
            .map_err(BuildError::Io)?;

        if !status.success() {
            return Err(BuildError::BuildFailed);
        }

        let lib_path = {
            #[cfg(target_os = "linux")]
            let name = format!("lib{}.so", library_name);
            #[cfg(target_os = "macos")]
            let name = format!("lib{library_name}.dylib");
            #[cfg(target_os = "windows")]
            let name = format!("lib{}.dll", library_name);

            build_dir.join("target").join("release").join(name)
        };

        let lib = unsafe { Library::new(&lib_path).map_err(|_| BuildError::LibraryLoadFailed)? };
        Ok(lib)
    }

    pub fn build(code: String, deps: String) -> Result<Library, BuildError> {
        let dir = TempDir::new()?;
        let dir_path = dir.path().to_path_buf();
        let lib = Self::build_named(code, deps, &dir_path)?;
        std::mem::forget(dir);
        Ok(lib)
    }
}
