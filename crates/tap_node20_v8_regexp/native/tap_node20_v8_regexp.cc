#include <climits>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "libplatform/libplatform.h"
#include "v8.h"

namespace {

std::once_flag init_once;
std::unique_ptr<v8::Platform> platform;

void init_v8() {
  std::call_once(init_once, []() {
    v8::V8::InitializeICUDefaultLocation(nullptr);
    v8::V8::InitializeExternalStartupData(nullptr);
    platform = v8::platform::NewDefaultPlatform();
    v8::V8::InitializePlatform(platform.get());
    v8::V8::Initialize();
  });
}

v8::MaybeLocal<v8::String> new_v8_string(
  v8::Isolate* isolate,
  const unsigned char* data,
  size_t len
) {
  if (len > static_cast<size_t>(INT_MAX)) {
    return {};
  }
  return v8::String::NewFromUtf8(
    isolate,
    reinterpret_cast<const char*>(data),
    v8::NewStringType::kNormal,
    static_cast<int>(len)
  );
}

class Matcher {
public:
  Matcher() {
    init_v8();

    allocator_.reset(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    v8::Isolate::CreateParams create_params;
    create_params.array_buffer_allocator = allocator_.get();
    isolate_ = v8::Isolate::New(create_params);

    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_);
    auto context = v8::Context::New(isolate_);
    v8::Context::Scope context_scope(context);

    const char* source =
      "(function(pattern, haystack) {"
      "  try {"
      "    const re = new RegExp(pattern, 'g');"
      "    const matches = String(haystack).match(re);"
      "    return matches === null ? -1 : matches.length;"
      "  } catch (_) {"
      "    return -1;"
      "  }"
      "})";

    v8::Local<v8::String> source_string;
    if (!v8::String::NewFromUtf8(isolate_, source, v8::NewStringType::kNormal)
      .ToLocal(&source_string)) {
      return;
    }

    v8::Local<v8::Script> script;
    if (!v8::Script::Compile(context, source_string).ToLocal(&script)) {
      return;
    }

    v8::Local<v8::Value> value;
    if (!script->Run(context).ToLocal(&value) || !value->IsFunction()) {
      return;
    }

    context_.Reset(isolate_, context);
    matcher_.Reset(isolate_, value.As<v8::Function>());
    ready_ = true;
  }

  ~Matcher() {
    matcher_.Reset();
    context_.Reset();
    if (isolate_ != nullptr) {
      isolate_->Dispose();
      isolate_ = nullptr;
    }
  }

  int count(
    const unsigned char* pattern,
    size_t pattern_len,
    const unsigned char* haystack,
    size_t haystack_len,
    size_t* out_count
  ) {
    if (!ready_ || out_count == nullptr) {
      return 0;
    }

    v8::Isolate::Scope isolate_scope(isolate_);
    v8::HandleScope handle_scope(isolate_);
    auto context = context_.Get(isolate_);
    v8::Context::Scope context_scope(context);
    auto matcher = matcher_.Get(isolate_);

    v8::Local<v8::String> pattern_string;
    if (!new_v8_string(isolate_, pattern, pattern_len).ToLocal(&pattern_string)) {
      return 0;
    }

    v8::Local<v8::String> haystack_string;
    if (!new_v8_string(isolate_, haystack, haystack_len).ToLocal(&haystack_string)) {
      return 0;
    }

    v8::Local<v8::Value> args[] = {pattern_string, haystack_string};
    v8::TryCatch try_catch(isolate_);
    v8::Local<v8::Value> value;
    if (!matcher->Call(context, v8::Undefined(isolate_), 2, args).ToLocal(&value)) {
      return 0;
    }
    if (try_catch.HasCaught() || !value->IsInt32()) {
      return 0;
    }

    int32_t count = value.As<v8::Int32>()->Value();
    if (count < 0) {
      return 0;
    }

    *out_count = static_cast<size_t>(count);
    return 1;
  }

private:
  std::unique_ptr<v8::ArrayBuffer::Allocator> allocator_;
  v8::Isolate* isolate_ = nullptr;
  v8::Global<v8::Context> context_;
  v8::Global<v8::Function> matcher_;
  bool ready_ = false;
};

thread_local std::unique_ptr<Matcher> matcher;

} // namespace

extern "C" const char* tap_node20_v8_version() {
  init_v8();
  return v8::V8::GetVersion();
}

extern "C" int tap_node20_v8_global_match_count(
  const unsigned char* pattern,
  size_t pattern_len,
  const unsigned char* haystack,
  size_t haystack_len,
  size_t* out_count
) {
  if (pattern == nullptr || haystack == nullptr) {
    return 0;
  }
  if (!matcher) {
    matcher = std::make_unique<Matcher>();
  }
  return matcher->count(pattern, pattern_len, haystack, haystack_len, out_count);
}
