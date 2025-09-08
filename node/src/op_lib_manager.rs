use std::collections::HashMap;

use libloading::Library;

use crate::LibName;

#[derive(Default, Debug)]
pub(crate) struct OpLibrary {
    container: HashMap<LibName, Library>,
}

impl OpLibrary {
    pub(crate) fn add_lib(&mut self, name: LibName, library: Library) {
        self.container.insert(name, library);
    }

    pub(crate) fn get_lib(&self, lib_name: &str) -> &Library {
        self.container
            .get(lib_name)
            .expect(&format!("Library {lib_name} not found"))
    }

    pub(crate) fn num_libs(&self) -> usize {
        self.container.len()
    }
}
