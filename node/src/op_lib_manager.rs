use std::collections::HashMap;

use libloading::Library;
use reactor_actor::ActorSpawnCB;
use tracing_shared::SharedLogger;

use crate::{LibName, SetupSharedLogger};

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

    pub(crate) fn get_lib(&self, lib_name: &str) -> &Library {
        &self
            .container
            .get(lib_name)
            .unwrap_or_else(|| panic!("Library {lib_name} not found"))
            .0
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

    pub(crate) fn get_op(
        &self,
        lib_name: String,
        op_name: String,
    ) -> libloading::Symbol<'_, ActorSpawnCB> {
        unsafe {
            let lib = self.get_lib(&lib_name);
            let shared_logger: libloading::Symbol<SetupSharedLogger> =
                lib.get(b"setup_shared_logger_ref").unwrap();
            let logger = SharedLogger::new();
            shared_logger(logger);
            let op: libloading::Symbol<ActorSpawnCB> = lib.get(op_name.as_bytes()).unwrap();
            op
        }
    }
}
