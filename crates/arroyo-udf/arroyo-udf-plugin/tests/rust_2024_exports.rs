//! Compile the public dynamic-plugin macros in the compiler's Rust 2024 edition.
//! The local_udf host tests do not emit dynamically exported symbols.
use arroyo_udf_plugin::udf;

#[udf]
fn increment(value: i64) -> i64 {
    value + 1
}

#[udf]
async fn async_increment(value: i64) -> i64 {
    value + 1
}

#[test]
fn public_sync_udf_compiles() {
    assert_eq!(increment(41), 42);
}

#[tokio::test]
async fn public_async_udf_compiles() {
    assert_eq!(async_increment(41).await, 42);
}
