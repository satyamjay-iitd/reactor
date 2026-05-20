use std::collections::HashMap;

use serde_json::Value;

pub trait CodeGenerator {
    type Error: std::error::Error;

    /// Generates rust code for a given operator.
    ///
    /// Specification of the generated rust code:-
    /// 1. It must have the following function:-
    ///    `async fn actor_callback(mpsc::UnboundedReceiver<ControlInst>, mpsc::UnboundedSender<ControlReq>, &str)`
    /// 2. Generated Cargo.toml must generate the library as cdylib.
    ///
    /// # Parameters
    /// - `op_name`: The name of the operator for which code should be generated.
    /// - `args`: A map of additional arguments (as key-value pairs) to customize code generation.
    ///
    /// # Returns
    /// A tuple containing:
    /// - The generated code as a `String`.
    /// - Cargo.toml for the generated code.
    fn generate(&self, args: HashMap<String, Value>) -> Result<(String, String), Self::Error>;
}
