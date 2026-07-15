use std::{collections::BTreeSet, env, fs, path::PathBuf};

fn main() {
    for file in [
        "src/shim.c",
        "../../../DPUMesh/buffer.c",
        "../../../DPUMesh/comch_client.c",
        "../../../DPUMesh/comch_common.c",
        "../../../DPUMesh/comch_consumer.c",
        "../../../DPUMesh/comch_msgq.c",
        "../../../DPUMesh/comch_server.c",
        "../../../DPUMesh/common.c",
        "../../../DPUMesh/dma.c",
        "../../../DPUMesh/dpa.c",
        "../../../DPUMesh/object.c",
        "../../../DPUMesh/build/device/dpa_kernel.a",
        "../../../DPUMesh/buffer.h",
        "../../../DPUMesh/comch_client.h",
        "../../../DPUMesh/comch_common.h",
        "../../../DPUMesh/comch_consumer.h",
        "../../../DPUMesh/comch_msgq.h",
        "../../../DPUMesh/comch_server.h",
        "../../../DPUMesh/common.h",
        "../../../DPUMesh/dma.h",
        "../../../DPUMesh/object.h",
        "../../../DPUMesh/dpa.h",
    ] {
        println!("cargo:rerun-if-changed={file}");
    }

    let libs = [
        "doca-common",
        "doca-comch",
        "doca-dma",
        "doca-aes-gcm",
        "doca-sha",
        "doca-dpa",
        "libflexio",
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
        .file("../../../DPUMesh/buffer.c")
        .file("../../../DPUMesh/comch_client.c")
        .file("../../../DPUMesh/comch_common.c")
        .file("../../../DPUMesh/comch_consumer.c")
        .file("../../../DPUMesh/comch_msgq.c")
        .file("../../../DPUMesh/comch_server.c")
        .file("../../../DPUMesh/common.c")
        .file("../../../DPUMesh/dma.c")
        .file("../../../DPUMesh/dpa.c")
        .file("../../../DPUMesh/object.c")
        .flag_if_supported("-Wno-deprecated-declarations")
        .define("ALLOW_EXPERIMENTAL_API", None)
        .define("DOCA_ALLOW_EXPERIMENTAL_API", None)
        .define("FLEXIO_ALLOW_EXPERIMENTAL_API", None);

    for path in include_paths {
        build.include(path);
    }

    build.include("../../../DPUMesh");

    build.compile("dmesh_doca_shim");

    let dpa_kernel = PathBuf::from("../../../DPUMesh/build/device/dpa_kernel.a")
        .canonicalize()
        .unwrap_or_else(|error| panic!("failed to find DPUMesh DPA kernel archive: {error}"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set"));
    let dpa_kernel_link = out_dir.join("libdpa_kernel.a");
    fs::copy(&dpa_kernel, &dpa_kernel_link)
        .unwrap_or_else(|error| panic!("failed to copy DPA kernel archive for linking: {error}"));
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=dpa_kernel");
}
