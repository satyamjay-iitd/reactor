use std::collections::HashMap;

use libloading::Library;
use reactor_actor::ActorSpawnCB;
use tracing_shared::SharedLogger;

use crate::{LibName, SetupSharedLogger, SpawnError};

#[derive(Default, Debug)]
pub(crate) struct OpLibrary {
    container: HashMap<LibName, (Library, Vec<String>)>,
    /// The code generation arguments of the libraries this node compiled.
    #[cfg_attr(not(feature = "dynop"), allow(dead_code))]
    compiled_args: HashMap<LibName, String>,
}

impl OpLibrary {
    /// Registers a library; `false` if it exports no operators (`get_registered`).
    pub(crate) fn add_lib(&mut self, name: LibName, library: Library) -> bool {
        let registered = unsafe {
            if let Ok(get_registered) =
                library.get::<libloading::Symbol<fn() -> Vec<String>>>(b"get_registered")
            {
                get_registered()
            } else {
                return false;
            }
        };
        self.container.insert(name, (library, registered));
        true
    }

    /// Registers a library this node compiled from `args`.
    #[cfg(feature = "dynop")]
    pub(crate) fn add_compiled(&mut self, name: LibName, library: Library, args: String) {
        if self.add_lib(name.clone(), library) {
            self.compiled_args.insert(name, args);
        }
    }

    /// The code generation arguments of a loaded library this node compiled; `Some("")` for one
    /// loaded from the operator directory, `None` if no library has this name.
    /// Unregisters the libraries this node compiled; returns their names. Their code stays
    /// mapped until the process exits: each runs threads that never end (its runtime), so
    /// unmapping it could crash the node.
    #[cfg(feature = "dynop")]
    pub(crate) fn forget_compiled(&mut self) -> Vec<LibName> {
        let mut names: Vec<LibName> = self.compiled_args.drain().map(|(name, _)| name).collect();
        for name in &names {
            if let Some((library, _)) = self.container.remove(name) {
                std::mem::forget(library);
            }
        }
        names.sort();
        names
    }

    #[cfg(feature = "dynop")]
    pub(crate) fn loaded_args(&self, name: &str) -> Option<&str> {
        self.container.contains_key(name).then(|| {
            self.compiled_args
                .get(name)
                .map(String::as_str)
                .unwrap_or("")
        })
    }

    pub(crate) fn get_lib(&self, lib_name: &str) -> Option<&Library> {
        self.container.get(lib_name).map(|(lib, _)| lib)
    }

    pub(crate) fn num_libs(&self) -> usize {
        self.container.len()
    }

    pub(crate) fn lib_names(&self) -> HashMap<LibName, Vec<String>> {
        self.container
            .iter()
            .map(|(name, (_, ops))| (name.clone(), ops.clone()))
            .collect()
    }

    /// The spawn function of an operator, after sharing the node's logger with its library.
    pub(crate) fn get_op(
        &self,
        lib_name: &str,
        op_name: &str,
    ) -> Result<libloading::Symbol<'_, ActorSpawnCB>, SpawnError> {
        let lib = self
            .get_lib(lib_name)
            .ok_or_else(|| SpawnError::LibraryNotFound(lib_name.to_string()))?;
        let not_found = || SpawnError::OperatorNotFound {
            lib: lib_name.to_string(),
            op: op_name.to_string(),
        };
        unsafe {
            let op: libloading::Symbol<ActorSpawnCB> =
                lib.get(op_name.as_bytes()).map_err(|_| not_found())?;
            // operator libraries export this; tolerate one that does not
            if let Ok(shared_logger) = lib.get::<SetupSharedLogger>(b"setup_shared_logger_ref") {
                shared_logger(SharedLogger::new());
            }
            Ok(op)
        }
    }
}
