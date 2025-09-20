extern "C" int re2_compile_ok(const char* /*pattern*/) {
  // Fallback: accept all patterns when RE2 is unavailable at build time
  return 1;
}

