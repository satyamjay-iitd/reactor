use std::collections::HashMap;

use libloading::Library;
use reactor_actor::ActorSpawnCB;
use tracing_shared::SharedLogger;

use crate::{LibName, SetupSharedLogger, SpawnError};

#[derive(Default, Debug)]
pub(crate) struct OpLibrary {
    container: HashMap<LibName, (Library, Vec<String>)>,
}

impl OpLibrary {
    pub(crate) fn add_lib(&mut self, name: LibName, library: Library) {
        let registered = unsafe {
            if let Ok(get_registered) =
                library.get::<libloading::Symbol<fn() -> Vec<String>>>(b"get_registered")
            {
                get_registered()
            } else {
                return;
            }
        };
        self.container.insert(name, (library, registered));
    }

    pub(crate) fn get_lib(&self, lib_name: &str) -> Option<&Library> {
        self.container.get(lib_name).map(|(lib, _)| lib)
    }

    pub(crate) fn has_lib(&self, name: &str) -> bool {
        self.container.contains_key(name)
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
