//! Rejection of pipeline UDFs that would shadow a trusted builtin.
//!
//! Physical plans serialize scalar functions by name, so a user UDF whose
//! name matches a built-in ANSI SQL/JSON kernel would be deserialized in
//! place of the trusted implementation on the worker. The planner refuses to
//! create such UDFs; this module is the worker-side defense for pipeline
//! configs that reach the worker by any other path.

use anyhow::{Result, anyhow};

/// Reject a pipeline UDF name that shadows a built-in ANSI SQL/JSON kernel.
///
/// `kind` names the loading path in the error (e.g. `"dylib"`, `"Python"`,
/// `"local"`) so the failure points at the right config entry.
pub(crate) fn reject_shadowing_udf_name(kind: &str, name: &str) -> Result<()> {
    if arroyo_planner::sql_json::kernels::is_reserved_sql_json_udf_name(name) {
        return Err(anyhow!(
            "pipeline {kind} UDF '{name}' shadows the built-in ANSI SQL/JSON function of the same name"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::reject_shadowing_udf_name;

    #[test]
    fn every_reserved_name_is_rejected_for_every_kind() {
        for kind in ["dylib", "Python", "local"] {
            for name in [
                "json_value",
                "json_value_boolean",
                "json_value_double",
                "json_query",
                "json_exists",
                "json_object",
                // Case-insensitive, matching the planner's check.
                "JSON_VALUE",
                "Json_Object",
            ] {
                let error = reject_shadowing_udf_name(kind, name).unwrap_err();
                let message = error.to_string();
                assert!(message.contains("shadows"), "{message}");
                assert!(message.contains(name), "{message}");
                assert!(message.contains(kind), "{message}");
            }
        }
    }

    #[test]
    fn non_reserved_names_pass() {
        for name in ["my_json_value", "json_value2", "extract_json", "json_get"] {
            reject_shadowing_udf_name("dylib", name).unwrap();
        }
    }
}
