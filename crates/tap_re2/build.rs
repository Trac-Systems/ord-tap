use std::{env, path::PathBuf};
use std::panic;

fn main() {
  // Try to build vendored RE2 if present; otherwise fall back to system RE2
  let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
  let vendor_dir = env::var("TAP_RE2_VENDOR").map(PathBuf::from)
    .ok()
    .filter(|p| p.exists())
    .unwrap_or_else(|| crate_dir.join("vendor").join("re2"));

  if vendor_dir.exists() {
    // Use CMake to build RE2 statically, optionally via vendored Abseil
    let absl_dir = crate_dir.join("vendor").join("abseil-cpp");
    if !absl_dir.exists() {
      println!("cargo:warning=Vendored abseil-cpp not found; falling back to system RE2. See README for instructions.");
    } else {
      let vendored_ok = panic::catch_unwind(|| {
        // Make RE2 pick up vendored Abseil via third_party path
        let tp_dir = vendor_dir.join("third_party");
        let re2_absl = tp_dir.join("abseil-cpp");
        if !tp_dir.exists() { let _ = std::fs::create_dir_all(&tp_dir); }
        if !re2_absl.exists() {
          #[cfg(unix)] {
            use std::os::unix::fs::symlink;
            let _ = symlink(&absl_dir, &re2_absl);
          }
        }
        // Configure/build RE2
        let dst_re2 = cmake::Config::new(&vendor_dir)
          .define("CMAKE_POSITION_INDEPENDENT_CODE", "ON")
          .define("CMAKE_BUILD_TYPE", "Release")
          .define("RE2_BUILD_TESTING", "OFF")
          .define("BUILD_SHARED_LIBS", "OFF")
          .define("CMAKE_PREFIX_PATH", absl_dir.join("lib").join("cmake").join("absl"))
          .build();
        let re2_lib_dir = dst_re2.join("lib");

        // Build wrapper and link statically
        let mut wrap = cc::Build::new();
        wrap.cpp(true)
          .file("src/wrapper.cc")
          .include(&vendor_dir)
          .flag_if_supported("-std=c++11")
          .compile("tap_re2_wrapper");
        println!("cargo:rustc-link-search=native={}", re2_lib_dir.display());
        println!("cargo:rustc-link-lib=static=re2");
      }).is_ok();
      if vendored_ok { return; }
      println!("cargo:warning=Vendored RE2 static build failed; falling back to system RE2. Install instructions:");
      println!("cargo:warning=- macOS: brew install re2");
      println!("cargo:warning=- Debian/Ubuntu: sudo apt-get install -y libre2-dev");
      println!("cargo:warning=- Alpine: apk add re2 re2-dev");
      println!("cargo:warning=- Windows (MSVC): add RE2 include/lib to INCLUDE and LIB env vars");
    }
  } else {
    println!("cargo:warning=Vendored RE2 not present; using system RE2. See README for setup.");
  }

  // System RE2 fallback path: compile wrapper and link dynamically
  let mut build = cc::Build::new();
  build.cpp(true)
    .file("src/wrapper.cc")
    .flag_if_supported("-std=c++11")
    .flag_if_supported("-Wno-unused-parameter")
    .compile("tap_re2_wrapper");
  println!("cargo:rustc-link-lib=re2");
}
