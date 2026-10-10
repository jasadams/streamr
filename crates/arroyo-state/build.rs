fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=ROCKSDB_LIB_DIR");
    // librocksdb-sys skips its C++ runtime directive when using an external
    // archive. Propagate the Debian image's runtime to our final executables.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux")
        && std::env::var_os("ROCKSDB_LIB_DIR").is_some()
    {
        println!("cargo:rustc-link-lib=dylib=stdc++");
    }
}
