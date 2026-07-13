use std::collections::BTreeSet;

fn main() {
    let libs = [
        "doca-common",
        "doca-dma",
        "doca-aes-gcm",
        "doca-sha",
        "doca-dpa",
    ];

    let mut include_paths = BTreeSet::new();
    for lib in libs {
        let found = pkg_config::Config::new()
            .probe(lib)
            .unwrap_or_else(|error| panic!("failed to find {lib} with pkg-config: {error}"));
        include_paths.extend(found.include_paths);
    }

    let mut build = cc::Build::new();
    build
        .file("src/shim.c")
        .flag_if_supported("-Wno-deprecated-declarations")
        .define("ALLOW_EXPERIMENTAL_API", None)
        .define("FLEXIO_ALLOW_EXPERIMENTAL_API", None);

    for path in include_paths {
        build.include(path);
    }

    build.compile("linkerd_doca_shim");
}
