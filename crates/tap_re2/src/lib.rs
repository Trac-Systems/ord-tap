use libc::{c_char, c_int};
use std::ffi::CString;

extern "C" {
  fn re2_compile_ok(pattern: *const c_char) -> c_int;
}

pub fn is_re2_valid(pattern: &str) -> bool {
  if pattern.is_empty() { return true; }
  match CString::new(pattern) {
    Ok(cstr) => unsafe { re2_compile_ok(cstr.as_ptr()) != 0 },
    Err(_) => false,
  }
}

