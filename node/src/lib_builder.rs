use std::fs;

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
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Couldn't Load the Library")]
    LibraryLoadFailed,
    #[error("Code generation failed: {0}")]
    CodegenFailed(String),
    /// The node was started without a code generator (static operators only).
    #[error("This node cannot compile operators")]
    CompilationNotSupported,
    /// A loaded library cannot be replaced: actors may be running its code.
    #[error(
        "library '{0}' is already loaded, built from other arguments; clear the compile cache to replace it"
    )]
    LoadedWithOtherArgs(String),
    /// Compiled libraries cannot be unloaded while actors may be running their code.
    #[error("{0} actor(s) are running on this node; stop them first")]
    ActorsRunning(usize),
    /// Compiled libraries cannot be unloaded while libraries are being built.
    #[error("libraries are being built ({0}); cancel the builds first")]
    Building(String),
    /// A library of this name is being built.
    #[error("library '{0}' is already being built")]
    AlreadyBuilding(String),
    #[error("the build of library '{0}' was cancelled")]
    Cancelled(String),
}

/// Where compiled operator libraries are built, one directory per library. A library's files
/// are only needed until it is loaded: deleting them does not affect loaded libraries.
pub fn build_root() -> std::path::PathBuf {
    std::env::temp_dir().join("streamary_builds")
}

/// Deletes the build directories; returns the bytes freed.
pub fn clear_build_root() -> std::io::Result<u64> {
    fn size(path: &std::path::Path) -> u64 {
        let Ok(meta) = fs::symlink_metadata(path) else {
            return 0;
        };
        if meta.is_dir() {
            fs::read_dir(path)
                .into_iter()
                .flatten()
                .flatten()
                .map(|e| size(&e.path()))
                .sum()
        } else {
            meta.len()
        }
    }
    let root = build_root();
    let freed = size(&root);
    match fs::remove_dir_all(&root) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(freed),
    }
}

impl LibBuilder {
    /// Like [`Self::build_named`], but cancellable: dropping the future kills the build (cargo
    /// and the compiler processes it started).
    pub async fn build_named_async(
        code: String,
        deps: String,
        build_dir: &std::path::Path,
    ) -> Result<Library, BuildError> {
        let library_name = Self::prepare(code, deps, build_dir)?;
        let mut cargo = tokio::process::Command::new("cargo");
        cargo
            .args(["build", "--release"])
            .current_dir(build_dir)
            .kill_on_drop(true);
        // its own process group, so that a cancellation also reaches the compilers it runs
        #[cfg(unix)]
        cargo.process_group(0);
        let mut child = cargo.spawn().map_err(BuildError::Io)?;
        #[cfg(unix)]
        let mut group = KillGroupOnDrop(child.id());
        let status = child.wait().await.map_err(BuildError::Io)?;
        #[cfg(unix)]
        {
            group.0 = None; // finished: nothing to kill
        }
        if !status.success() {
            return Err(BuildError::BuildFailed);
        }
        Self::load(build_dir, &library_name)
    }

    /// Writes the crate into `build_dir`; returns the library's name.
    fn prepare(
        code: String,
        deps: String,
        build_dir: &std::path::Path,
    ) -> Result<String, BuildError> {
        let cargo_manifest = Manifest::from_str(&deps).map_err(|_| BuildError::InvalidCargoToml)?;
        let lib = cargo_manifest
            .lib
            .as_ref()
            .ok_or(BuildError::InvalidCargoToml)?;
        if !lib.crate_type.iter().any(|ct| ct == "cdylib") {
            return Err(BuildError::InvalidCargoToml);
        }
        let library_name = lib.name.clone().ok_or(BuildError::InvalidCargoToml)?;

        fs::create_dir_all(build_dir.join("src"))?;
        fs::write(build_dir.join("Cargo.toml"), deps)?;
        fs::write(build_dir.join("src").join("lib.rs"), code)?;
        Ok(library_name)
    }

    /// Loads the built library.
    fn load(build_dir: &std::path::Path, library_name: &str) -> Result<Library, BuildError> {
        let lib_path = {
            #[cfg(target_os = "linux")]
            let name = format!("lib{}.so", library_name);
            #[cfg(target_os = "macos")]
            let name = format!("lib{library_name}.dylib");
            #[cfg(target_os = "windows")]
            let name = format!("lib{}.dll", library_name);

            build_dir.join("target").join("release").join(name)
        };

        // Load a copy with a name of its own: the dynamic loader identifies libraries by path, so
        // after an unload (whose code stays mapped, see `OpLibrary::forget_compiled`) loading a
        // rebuild from the same path would return the old code.
        static LOADS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = LOADS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let loaded_dir = build_dir.join("loaded");
        fs::create_dir_all(&loaded_dir)?;
        let file_name = lib_path
            .file_name()
            .expect("library file name")
            .to_string_lossy();
        let copy = loaded_dir.join(format!("{}-{n}-{file_name}", std::process::id()));
        fs::copy(&lib_path, &copy)?;

        let lib = unsafe { Library::new(&copy).map_err(|_| BuildError::LibraryLoadFailed)? };
        Ok(lib)
    }
}

/// Kills a process group when dropped (unless its id was taken out).
#[cfg(unix)]
struct KillGroupOnDrop(Option<u32>);

#[cfg(unix)]
impl Drop for KillGroupOnDrop {
    fn drop(&mut self) {
        if let Some(pgid) = self.0 {
            // the group's id is its leader's pid (`process_group(0)`)
            unsafe { libc::killpg(pgid as libc::pid_t, libc::SIGKILL) };
        }
    }
}
