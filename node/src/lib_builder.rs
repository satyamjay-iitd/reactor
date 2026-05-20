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
    pub fn build(code: String, deps: String) -> Result<Library, BuildError> {
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

        // Create a temporary directory for the crate
        let dir = TempDir::new()?;
        let dir_path = dir.path();

        // Write Cargo.toml
        fs::write(dir_path.join("Cargo.toml"), deps)?;

        // Write src/lib.rs
        let src_path = dir_path.join("src");
        fs::create_dir_all(&src_path)?;
        fs::write(src_path.join("lib.rs"), code)?;

        // Build with cargo
        let status = Command::new("cargo")
            .args(["build", "--release"])
            // .args(["build", "--offline", "--release"])
            .current_dir(dir_path)
            .status()
            .map_err(BuildError::Io)?;

        if !status.success() {
            return Err(BuildError::BuildFailed);
        }

        // Determine the path to the compiled library
        let target_dir = dir_path.join("target").join("release");

        let lib_path = {
            #[cfg(target_os = "linux")]
            let name = format!("lib{}.so", library_name);
            #[cfg(target_os = "macos")]
            let name = format!("lib{library_name}.dylib");
            #[cfg(target_os = "windows")]
            let name = format!("lib{}.dll", library_name);

            target_dir.join(name)
        };

        // Load the library using libloading
        let lib = unsafe { Library::new(&lib_path).map_err(|_| BuildError::LibraryLoadFailed)? };

        // We don’t return the TempDir because dropping it would delete the .so
        std::mem::forget(dir);

        Ok(lib)
    }
}
