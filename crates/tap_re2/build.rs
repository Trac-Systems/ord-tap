use std::{
  env,
  path::{Path, PathBuf},
};

const RE2_VENDOR_VERSION: &str = "2024-06-01";

const RE2_SOURCES: &[&str] = &[
  "vendor/re2/re2/bitmap256.cc",
  "vendor/re2/re2/bitstate.cc",
  "vendor/re2/re2/compile.cc",
  "vendor/re2/re2/dfa.cc",
  "vendor/re2/re2/filtered_re2.cc",
  "vendor/re2/re2/mimics_pcre.cc",
  "vendor/re2/re2/nfa.cc",
  "vendor/re2/re2/onepass.cc",
  "vendor/re2/re2/parse.cc",
  "vendor/re2/re2/perl_groups.cc",
  "vendor/re2/re2/prefilter.cc",
  "vendor/re2/re2/prefilter_tree.cc",
  "vendor/re2/re2/prog.cc",
  "vendor/re2/re2/re2.cc",
  "vendor/re2/re2/regexp.cc",
  "vendor/re2/re2/set.cc",
  "vendor/re2/re2/simplify.cc",
  "vendor/re2/re2/tostring.cc",
  "vendor/re2/re2/unicode_casefold.cc",
  "vendor/re2/re2/unicode_groups.cc",
  "vendor/re2/util/pcre.cc",
  "vendor/re2/util/rune.cc",
  "vendor/re2/util/strutil.cc",
];

const ABSL_SOURCES: &[&str] = &[
  "vendor/abseil-cpp/absl/base/internal/cycleclock.cc",
  "vendor/abseil-cpp/absl/base/internal/low_level_alloc.cc",
  "vendor/abseil-cpp/absl/base/internal/raw_logging.cc",
  "vendor/abseil-cpp/absl/base/internal/spinlock.cc",
  "vendor/abseil-cpp/absl/base/internal/spinlock_wait.cc",
  "vendor/abseil-cpp/absl/base/internal/strerror.cc",
  "vendor/abseil-cpp/absl/base/internal/sysinfo.cc",
  "vendor/abseil-cpp/absl/base/internal/thread_identity.cc",
  "vendor/abseil-cpp/absl/base/internal/throw_delegate.cc",
  "vendor/abseil-cpp/absl/base/internal/unscaledcycleclock.cc",
  "vendor/abseil-cpp/absl/container/internal/raw_hash_set.cc",
  "vendor/abseil-cpp/absl/debugging/internal/address_is_readable.cc",
  "vendor/abseil-cpp/absl/debugging/internal/demangle.cc",
  "vendor/abseil-cpp/absl/debugging/internal/elf_mem_image.cc",
  "vendor/abseil-cpp/absl/debugging/internal/examine_stack.cc",
  "vendor/abseil-cpp/absl/debugging/internal/vdso_support.cc",
  "vendor/abseil-cpp/absl/debugging/stacktrace.cc",
  "vendor/abseil-cpp/absl/debugging/symbolize.cc",
  "vendor/abseil-cpp/absl/flags/commandlineflag.cc",
  "vendor/abseil-cpp/absl/flags/internal/commandlineflag.cc",
  "vendor/abseil-cpp/absl/flags/internal/flag.cc",
  "vendor/abseil-cpp/absl/flags/internal/private_handle_accessor.cc",
  "vendor/abseil-cpp/absl/flags/internal/program_name.cc",
  "vendor/abseil-cpp/absl/flags/marshalling.cc",
  "vendor/abseil-cpp/absl/flags/reflection.cc",
  "vendor/abseil-cpp/absl/flags/usage_config.cc",
  "vendor/abseil-cpp/absl/hash/internal/city.cc",
  "vendor/abseil-cpp/absl/hash/internal/hash.cc",
  "vendor/abseil-cpp/absl/hash/internal/low_level_hash.cc",
  "vendor/abseil-cpp/absl/log/internal/globals.cc",
  "vendor/abseil-cpp/absl/log/internal/log_format.cc",
  "vendor/abseil-cpp/absl/log/internal/log_message.cc",
  "vendor/abseil-cpp/absl/log/internal/log_sink_set.cc",
  "vendor/abseil-cpp/absl/log/internal/nullguard.cc",
  "vendor/abseil-cpp/absl/log/internal/proto.cc",
  "vendor/abseil-cpp/absl/log/globals.cc",
  "vendor/abseil-cpp/absl/log/log_sink.cc",
  "vendor/abseil-cpp/absl/numeric/int128.cc",
  "vendor/abseil-cpp/absl/strings/ascii.cc",
  "vendor/abseil-cpp/absl/strings/charconv.cc",
  "vendor/abseil-cpp/absl/strings/internal/charconv_bigint.cc",
  "vendor/abseil-cpp/absl/strings/internal/charconv_parse.cc",
  "vendor/abseil-cpp/absl/strings/internal/memutil.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/arg.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/bind.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/extension.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/float_conversion.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/output.cc",
  "vendor/abseil-cpp/absl/strings/internal/str_format/parser.cc",
  "vendor/abseil-cpp/absl/strings/match.cc",
  "vendor/abseil-cpp/absl/strings/numbers.cc",
  "vendor/abseil-cpp/absl/strings/str_cat.cc",
  "vendor/abseil-cpp/absl/strings/str_split.cc",
  "vendor/abseil-cpp/absl/strings/string_view.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/create_thread_identity.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/futex_waiter.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/graphcycles.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/kernel_timeout.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/per_thread_sem.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/pthread_waiter.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/sem_waiter.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/stdcpp_waiter.cc",
  "vendor/abseil-cpp/absl/synchronization/internal/waiter_base.cc",
  "vendor/abseil-cpp/absl/synchronization/mutex.cc",
  "vendor/abseil-cpp/absl/time/clock.cc",
  "vendor/abseil-cpp/absl/time/duration.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_fixed.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_if.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_impl.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_info.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_libc.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_lookup.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/time_zone_posix.cc",
  "vendor/abseil-cpp/absl/time/internal/cctz/src/zone_info_source.cc",
  "vendor/abseil-cpp/absl/time/time.cc",
];

const ABSL_WINDOWS_SOURCES: &[&str] = &[
  "vendor/abseil-cpp/absl/synchronization/internal/win32_waiter.cc",
];

fn main() {
  println!("cargo:rustc-check-cfg=cfg(tap_re2_stub)");
  println!("cargo:rustc-check-cfg=cfg(tap_re2_real)");
  println!("cargo:rustc-check-cfg=cfg(tap_re2_system)");
  println!("cargo:rustc-check-cfg=cfg(tap_re2_vendored)");
  println!("cargo:rustc-check-cfg=cfg(tap_re2_vendored_20240601)");
  println!("cargo:rerun-if-env-changed=TAP_RE2_USE_STUB");
  println!("cargo:rerun-if-env-changed=TAP_RE2_USE_SYSTEM");
  println!("cargo:rerun-if-env-changed=TAP_RE2_ALLOW_SYSTEM_RELEASE");

  let profile = env::var("PROFILE").unwrap_or_else(|_| String::from("release"));

  if env::var("TAP_RE2_USE_STUB").ok().as_deref() == Some("1") {
    if profile == "release" {
      panic!("TAP_RE2_USE_STUB=1 is not allowed for release builds.");
    }
    build_stub();
    return;
  }

  if env::var("TAP_RE2_USE_SYSTEM").ok().as_deref() == Some("1") {
    if profile == "release"
      && env::var("TAP_RE2_ALLOW_SYSTEM_RELEASE").ok().as_deref() != Some("1")
    {
      panic!(
        "System RE2 is not allowed for release builds. \
         Use the vendored RE2 parity build, or explicitly set \
         TAP_RE2_ALLOW_SYSTEM_RELEASE=1 for a non-parity experiment."
      );
    }
    build_system();
    return;
  }

  build_vendored();
}

fn build_stub() {
  println!("cargo:warning=TAP_RE2_USE_STUB=1 set; building DEV ONLY stub backend.");
  let mut build = cc::Build::new();
  build
    .cpp(true)
    .file("src/wrapper_fallback.cc")
    .flag_if_supported("-std=c++17")
    .flag_if_supported("/std:c++17")
    .compile("tap_re2_wrapper_stub");
  println!("cargo:rustc-cfg=tap_re2_stub");
}

fn build_system() {
  println!("cargo:warning=TAP_RE2_USE_SYSTEM=1 set; building DEV ONLY system RE2 backend.");
  let crate_dir = crate_dir();
  let mut build = cc::Build::new();
  build
    .cpp(true)
    .file(crate_dir.join("src/wrapper.cc"))
    .flag_if_supported("-std=c++17")
    .flag_if_supported("/std:c++17")
    .flag_if_supported("-Wno-unused-parameter")
    .define("NDEBUG", None)
    .define("NOMINMAX", None);
  for inc in ["/opt/homebrew/include", "/usr/local/include", "/usr/include"] {
    if PathBuf::from(inc).join("re2/re2.h").exists() {
      build.include(inc);
    }
  }
  build.compile("tap_re2_wrapper_system");
  for lib in ["/opt/homebrew/lib", "/usr/local/lib", "/usr/lib"] {
    if PathBuf::from(lib).join("libre2.a").exists()
      || PathBuf::from(lib).join("libre2.dylib").exists()
      || PathBuf::from(lib).join("libre2.so").exists()
      || PathBuf::from(lib).join("re2.lib").exists()
    {
      println!("cargo:rustc-link-search=native={}", lib);
    }
  }
  println!("cargo:rustc-link-lib=re2");
  println!("cargo:rustc-cfg=tap_re2_real");
  println!("cargo:rustc-cfg=tap_re2_system");
}

fn build_vendored() {
  let crate_dir = crate_dir();
  let re2_dir = crate_dir.join("vendor/re2");
  let absl_dir = crate_dir.join("vendor/abseil-cpp");
  ensure_file_contains(
    &re2_dir.join("MODULE.bazel"),
    &format!("version = \"{}\"", RE2_VENDOR_VERSION),
  );
  ensure_file_contains(&absl_dir.join("MODULE.bazel"), "version = \"20240116.2\"");

  let target = env::var("TARGET").unwrap_or_default();
  let is_windows = target.contains("windows");
  let is_linux = target.contains("linux") || target.contains("android");

  let mut build = cc::Build::new();
  build
    .cpp(true)
    .include(re2_dir)
    .include(absl_dir)
    .file(crate_dir.join("src/wrapper.cc"))
    .define("NDEBUG", None)
    .define("NOMINMAX", None)
    .flag_if_supported("-std=c++2a")
    .flag_if_supported("/std:c++20")
    .flag_if_supported("-Wall")
    .flag_if_supported("-Wextra")
    .flag_if_supported("-Wno-sign-compare")
    .flag_if_supported("-Wno-unused-parameter")
    .flag_if_supported("-Wno-missing-field-initializers")
    .flag_if_supported("-Wno-cast-function-type");

  if is_linux {
    build.flag_if_supported("-pthread");
    println!("cargo:rustc-link-lib=pthread");
  }

  for source in RE2_SOURCES
    .iter()
    .chain(ABSL_SOURCES)
    .chain(if is_windows {
      ABSL_WINDOWS_SOURCES
    } else {
      &[]
    })
  {
    build.file(crate_dir.join(source));
    println!("cargo:rerun-if-changed={}", crate_dir.join(source).display());
  }
  println!("cargo:rerun-if-changed={}", crate_dir.join("src/wrapper.cc").display());
  println!("cargo:rerun-if-changed={}", crate_dir.join("src/wrapper_fallback.cc").display());
  println!("cargo:rerun-if-changed={}", crate_dir.join("vendor/re2/MODULE.bazel").display());
  println!(
    "cargo:rerun-if-changed={}",
    crate_dir.join("vendor/abseil-cpp/MODULE.bazel").display()
  );

  build.compile("tap_re2_vendored");
  println!("cargo:rustc-cfg=tap_re2_real");
  println!("cargo:rustc-cfg=tap_re2_vendored");
  println!("cargo:rustc-cfg=tap_re2_vendored_20240601");
}

fn crate_dir() -> PathBuf {
  PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be set"))
}

fn ensure_file_contains(path: &Path, needle: &str) {
  let text = std::fs::read_to_string(path)
    .unwrap_or_else(|err| panic!("Could not read {}: {err}", path.display()));
  assert!(
    text.contains(needle),
    "{} must contain {needle:?} for TAP RE2 parity",
    path.display()
  );
}
