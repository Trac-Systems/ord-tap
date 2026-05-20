use sha2::{Digest, Sha256};
use std::{
  collections::HashMap,
  env,
  fs::{self, File},
  io::Read,
  path::{Path, PathBuf},
};

const V8_LIBS: &[&str] = &[
  "v8_snapshot",
  "v8_initializers",
  "v8_init",
  "v8_compiler",
  "v8_turboshaft",
  "v8_base_without_compiler",
  "v8_libplatform",
  "v8_libbase",
  "v8_zlib",
  "v8_libsampler",
  "icui18n",
  "icuucx",
  "icudata",
];

fn main() {
  let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
  let target = env::var("TARGET").unwrap();

  println!("cargo:rerun-if-env-changed=TAP_NODE20_V8_ARTIFACT_DIR");
  println!("cargo:rerun-if-env-changed=TAP_NODE20_V8_SOURCE_DIR");
  println!("cargo:rerun-if-env-changed=TAP_V8_FROM_SOURCE");

  let artifact = locate_artifact(&manifest_dir, &target);
  verify_required_files(&artifact);
  if artifact.requires_sha256 {
    verify_sha256s(&artifact.root);
  }

  compile_shim(&manifest_dir, &artifact.include_dir);
  link_v8(&target, &artifact.lib_dir);
}

struct Artifact {
  root: PathBuf,
  lib_dir: PathBuf,
  include_dir: PathBuf,
  requires_sha256: bool,
}

fn locate_artifact(manifest_dir: &Path, target: &str) -> Artifact {
  if let Ok(source_dir) = env::var("TAP_NODE20_V8_SOURCE_DIR") {
    let root = PathBuf::from(source_dir);
    return Artifact {
      lib_dir: root.join("out/Release"),
      include_dir: root.join("deps/v8/include"),
      root,
      requires_sha256: false,
    };
  }

  if env::var("TAP_V8_FROM_SOURCE").ok().as_deref() == Some("1") {
    panic!(
      "TAP_V8_FROM_SOURCE=1 is set, but TAP_NODE20_V8_SOURCE_DIR is missing. \
       Build Node.js v20.10.0 V8 archives first, then set TAP_NODE20_V8_SOURCE_DIR=/path/to/node-v20.10.0"
    );
  }

  let root = env::var("TAP_NODE20_V8_ARTIFACT_DIR")
    .map(PathBuf::from)
    .unwrap_or_else(|_| manifest_dir.join("vendor/node20-v8").join(target));

  Artifact {
    lib_dir: root.join("lib"),
    include_dir: root.join("include/v8"),
    root,
    requires_sha256: true,
  }
}

fn verify_required_files(artifact: &Artifact) {
  if !artifact.include_dir.join("v8.h").exists() {
    panic!(
      "missing exact Node 20.10 V8 headers at {}",
      artifact.include_dir.display()
    );
  }

  for lib in V8_LIBS {
    let path = artifact.lib_dir.join(static_lib_file_name(lib));
    if !path.exists() {
      panic!(
        "missing exact Node 20.10 V8 archive {}. Provide a checked artifact or set TAP_NODE20_V8_SOURCE_DIR.",
        path.display()
      );
    }
  }
}

fn verify_sha256s(root: &Path) {
  let manifest_path = root.join("SHA256SUMS");
  if !manifest_path.exists() {
    panic!(
      "missing SHA256SUMS for exact Node 20.10 V8 artifact at {}",
      root.display()
    );
  }

  let manifest = fs::read_to_string(&manifest_path).expect("read SHA256SUMS");
  let expected = parse_sha256s(&manifest);
  for lib in V8_LIBS {
    let rel = format!("lib/{}", static_lib_file_name(lib));
    let expected_hash = expected
      .get(&rel)
      .unwrap_or_else(|| panic!("missing SHA256SUMS entry for {rel}"));
    let actual_hash = sha256_hex(&root.join(&rel));
    if &actual_hash != expected_hash {
      panic!(
        "SHA256 mismatch for {rel}: expected {expected_hash}, got {actual_hash}"
      );
    }
  }
}

fn static_lib_file_name(lib: &str) -> String {
  if env::var("TARGET").unwrap().contains("windows-msvc") {
    format!("{lib}.lib")
  } else {
    format!("lib{lib}.a")
  }
}

fn parse_sha256s(manifest: &str) -> HashMap<String, String> {
  manifest
    .lines()
    .filter_map(|line| {
      let mut parts = line.split_whitespace();
      let hash = parts.next()?;
      let path = parts.next()?;
      Some((path.trim_start_matches("./").to_string(), hash.to_string()))
    })
    .collect()
}

fn sha256_hex(path: &Path) -> String {
  let mut file = File::open(path).unwrap_or_else(|err| panic!("open {}: {err}", path.display()));
  let mut hasher = Sha256::new();
  let mut buffer = [0u8; 1024 * 1024];
  loop {
    let read = file
      .read(&mut buffer)
      .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    if read == 0 {
      break;
    }
    hasher.update(&buffer[..read]);
  }
  let digest = hasher.finalize();
  format!("{digest:x}")
}

fn compile_shim(manifest_dir: &Path, include_dir: &Path) {
  let mut build = cc::Build::new();
  build
    .cpp(true)
    .std("c++17")
    .flag_if_supported("-Wno-unused-parameter")
    .include(include_dir)
    .file(manifest_dir.join("native/tap_node20_v8_regexp.cc"));

  build.compile("tap_node20_v8_regexp_shim");
}

fn link_v8(target: &str, lib_dir: &Path) {
  println!("cargo:rustc-link-search=native={}", lib_dir.display());
  for lib in V8_LIBS {
    println!("cargo:rustc-link-lib=static={lib}");
  }

  if target.contains("apple") {
    println!("cargo:rustc-link-lib=framework=CoreFoundation");
    println!("cargo:rustc-link-lib=framework=Security");
    println!("cargo:rustc-link-lib=z");
  } else if target.contains("linux") {
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=dl");
    println!("cargo:rustc-link-lib=pthread");
  } else if target.contains("windows-msvc") {
    println!("cargo:rustc-link-lib=dbghelp");
    println!("cargo:rustc-link-lib=winmm");
  }
}
