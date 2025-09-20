use std::{env, path::PathBuf};
use std::panic;

fn main() {
  // Silence cfg warnings for custom backends
  println!("cargo:rustc-check-cfg=cfg(tap_re2_stub)");
  println!("cargo:rustc-check-cfg=cfg(tap_re2_real)");
  // Force stub fallback via env for environments without RE2 toolchain
  if env::var("TAP_RE2_USE_STUB").ok().as_deref() == Some("1") {
    println!("cargo:warning=TAP_RE2_USE_STUB=1 set; building stub that accepts all patterns (DEV ONLY). Release parity requires real RE2.");
    let mut build = cc::Build::new();
    build.cpp(true)
      .file("src/wrapper_fallback.cc")
      .flag_if_supported("-std=c++17")
      .compile("tap_re2_wrapper_stub");
    // Expose cfg so runtime can report backend
    println!("cargo:rustc-cfg=tap_re2_stub");
    return;
  }
  // Try to build vendored RE2 if present; otherwise fall back to system RE2
  let crate_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
  let vendor_dir = env::var("TAP_RE2_VENDOR").map(PathBuf::from)
    .ok()
    .filter(|p| p.exists())
    .unwrap_or_else(|| crate_dir.join("vendor").join("re2"));
  let use_vendor = env::var("TAP_RE2_USE_VENDOR").ok().as_deref() == Some("1");

  if use_vendor && vendor_dir.exists() {
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
        let absl_third_party = vendor_dir.join("third_party").join("abseil-cpp");
        wrap.cpp(true)
          .file("src/wrapper.cc")
          .include(&vendor_dir)
          .include(&absl_third_party)
          .flag_if_supported("-std=c++17")
          .compile("tap_re2_wrapper");
        println!("cargo:rustc-link-search=native={}", re2_lib_dir.display());
        println!("cargo:rustc-link-lib=static=re2");
        println!("cargo:rustc-cfg=tap_re2_real");
      }).is_ok();
      if vendored_ok { return; }
      println!("cargo:warning=Vendored RE2 static build failed; falling back to system RE2. Install instructions:");
      println!("cargo:warning=- macOS: brew install re2");
      println!("cargo:warning=- Debian/Ubuntu: sudo apt-get install -y libre2-dev");
      println!("cargo:warning=- Alpine: apk add re2 re2-dev");
      println!("cargo:warning=- Windows (MSVC): add RE2 include/lib to INCLUDE and LIB env vars");
    }
  } else {
    println!("cargo:warning=Using system RE2 (preferred). Set TAP_RE2_USE_VENDOR=1 to force vendored build.");
  }

  // System RE2 fallback path: try to compile wrapper against system RE2; if that fails, build a stub
  let sys_ok = panic::catch_unwind(|| {
    let mut build = cc::Build::new();
    build.cpp(true)
      .file("src/wrapper.cc")
      .flag_if_supported("-std=c++17")
      .flag_if_supported("-Wno-unused-parameter");
    // Try common include paths for Homebrew/macOS and /usr/local
    for inc in [
      "/opt/homebrew/include",
      "/usr/local/include",
      "/usr/include",
    ] {
      if PathBuf::from(inc).join("re2/re2.h").exists() { build.include(inc); }
    }
    build.compile("tap_re2_wrapper");
    // Link search hints for common prefixes
    for lib in [
      "/opt/homebrew/lib",
      "/usr/local/lib",
      "/usr/lib",
    ] {
      if PathBuf::from(lib).join(if cfg!(target_os = "windows") { "re2.lib" } else { "libre2.a" }).exists()
        || PathBuf::from(lib).join("libre2.dylib").exists()
        || PathBuf::from(lib).join("libre2.so").exists() {
        println!("cargo:rustc-link-search=native={}", lib);
      }
    }
    // Allow environment overrides
    if let Ok(dir) = env::var("RE2_LIB_DIR") { println!("cargo:rustc-link-search=native={}", dir); }
    println!("cargo:rustc-link-lib=re2");
    println!("cargo:rustc-cfg=tap_re2_real");
  }).is_ok();
  if !sys_ok {
    // In release profile, do NOT silently fall back to stub unless explicitly opted in.
    let profile = env::var("PROFILE").unwrap_or_else(|_| String::from("release"));
    if profile == "release" {
      panic!(
        "RE2 not available for release build. Install RE2 or configure vendored RE2. \
         To bypass (DEV ONLY), explicitly set TAP_RE2_USE_STUB=1 to force a stub backend that over-accepts patterns."
      );
    }
    println!("cargo:warning=System RE2 not available; building stub that accepts all patterns (DEV ONLY). Install RE2 for full parity.");
    let mut build = cc::Build::new();
    build.cpp(true)
      .file("src/wrapper_fallback.cc")
      .flag_if_supported("-std=c++17")
      .compile("tap_re2_wrapper_stub");
    // No external lib link needed for stub
    println!("cargo:rustc-cfg=tap_re2_stub");
  }
}
