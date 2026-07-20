//! Build whisper.cpp only for the helper binary (feature-gated).
//! Main process never links against whisper.cpp.

fn main() {
    println!("cargo:rustc-check-cfg=cfg(whisper_cpp_linked)");
    println!("cargo:rustc-check-cfg=cfg(whisper_cpp_stub)");
    println!("cargo:rerun-if-changed=src/whisper_shim.c");
    println!("cargo:rerun-if-changed=build.rs");
    #[cfg(feature = "whisper-cpp")]
    {
        build_whisper_cpp();
    }
}

#[cfg(feature = "whisper-cpp")]
fn build_whisper_cpp() {
    use std::env;
    use std::path::PathBuf;

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let whisper_src = manifest_dir.join("vendor/whisper.cpp");
    let whisper_h = if whisper_src.join("include/whisper.h").is_file() {
        whisper_src.join("include")
    } else if whisper_src.join("whisper.h").is_file() {
        whisper_src.clone()
    } else {
        println!(
            "cargo:warning=whisper.cpp sources missing at {}; skip link",
            whisper_src.display()
        );
        println!("cargo:rustc-cfg=whisper_cpp_stub");
        return;
    };

    let dst = cmake::Config::new(&whisper_src)
        .define("BUILD_SHARED_LIBS", "OFF")
        .define("WHISPER_BUILD_TESTS", "OFF")
        .define("WHISPER_BUILD_EXAMPLES", "OFF")
        .define("WHISPER_BUILD_SERVER", "OFF")
        .define("GGML_NATIVE", "OFF")
        .profile("Release")
        .build();

    cc::Build::new()
        .file(manifest_dir.join("src/whisper_shim.c"))
        .include(&whisper_h)
        .include(dst.join("include"))
        .flag_if_supported("-std=c11")
        .compile("vc_whisper_shim");

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-search=native={}/lib64", dst.display());
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build").join("src").display()
    );
    println!(
        "cargo:rustc-link-search=native={}",
        dst.join("build").join("ggml").join("src").display()
    );

    println!("cargo:rustc-link-lib=static=vc_whisper_shim");
    println!("cargo:rustc-link-lib=static=whisper");
    println!("cargo:rustc-link-lib=static=ggml");
    println!("cargo:rustc-link-lib=static=ggml-base");
    println!("cargo:rustc-link-lib=static=ggml-cpu");
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-lib=dylib=m");
    println!("cargo:rustc-link-lib=dylib=pthread");
    println!("cargo:rustc-link-lib=dylib=gomp");
    println!("cargo:rustc-cfg=whisper_cpp_linked");
}
