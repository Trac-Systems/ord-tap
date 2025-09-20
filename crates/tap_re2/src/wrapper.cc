#include <re2/re2.h>

extern "C" int re2_compile_ok(const char* pattern) {
  // Construct RE2 and return ok() as int
  RE2 re(pattern);
  return re.ok() ? 1 : 0;
}

