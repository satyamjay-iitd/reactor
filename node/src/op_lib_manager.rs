use std::collections::HashMap;

use libloading::Library;

use crate::LibName;

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
}
