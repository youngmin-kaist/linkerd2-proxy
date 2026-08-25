use std::path::{Path, PathBuf};

fn main() {
    let has_pc = std::process::Command::new("pkg-config")
        .args(["--atleast-version=1.43.0", "libnghttp2"])
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    if !has_pc {
        // No -dev package: link against the versioned runtime SONAME by
        // creating a linker-visible `libnghttp2.so` symlink in OUT_DIR.
        let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
        let candidates = [
            "/lib/aarch64-linux-gnu/libnghttp2.so.14",
            "/lib/x86_64-linux-gnu/libnghttp2.so.14",
            "/usr/lib/aarch64-linux-gnu/libnghttp2.so.14",
            "/usr/lib/x86_64-linux-gnu/libnghttp2.so.14",
        ];
        if let Some(src) = candidates.iter().map(Path::new).find(|p| p.exists()) {
            let link = out.join("libnghttp2.so");
            let _ = std::fs::remove_file(&link);
            #[cfg(unix)]
            std::os::unix::fs::symlink(src, &link).expect("symlink libnghttp2.so");
            println!("cargo:rustc-link-search=native={}", out.display());
        } else {
            println!("cargo:warning=libnghttp2 runtime library not found; link will fail");
        }
    }
    println!("cargo:rustc-link-lib=dylib=nghttp2");
    println!("cargo:rerun-if-changed=build.rs");
}
