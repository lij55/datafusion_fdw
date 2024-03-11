use pgrx::prelude::*;

mod functions;
mod utils;

pgrx::pg_module_magic!();

extension_sql!(
    r#"
CREATE FUNCTION datafusion_fdw_handler()
RETURNS fdw_handler AS 'MODULE_PATHNAME', 'datafusion_fdw_handler' LANGUAGE C STRICT;
"#,
    name = "datafusion_fdw_handler"
);


#[pg_extern]
fn hello_datafusion_fdw() -> &'static str {
    "Hello, datafusion_fdw"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_datafusion_fdw() {
        assert_eq!("Hello, datafusion_fdw", crate::hello_datafusion_fdw());
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
