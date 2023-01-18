#include "ConsoleLog.h"

#include <cstdio>
#include <cstdlib>

void Print(const v8::FunctionCallbackInfo<v8::Value> & args);
void Quit(const v8::FunctionCallbackInfo<v8::Value> & args);

v8::Isolate * isolate_;

v8::Isolate * GetIsolate()
{
    return isolate_;
}

// Extracts a C string from a V8 Utf8Value.
const char * ToCString(const v8::String::Utf8Value & value)
{
    return *value ? *value : "<string conversion failed>";
}

class Console;

Console * UnwrapConsoleObject(v8::Handle<v8::Object> object);

class Console
{
public:
    Console() { }
    ~Console() { }
};

void log(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::String::Utf8Value str(GetIsolate(), args[0]);
    const char * cstr = ToCString(str);
    printf("%s\n", cstr);
}

void error(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    v8::String::Utf8Value str(GetIsolate(), args[0]);
    const char * cstr = ToCString(str);
    fprintf(stderr, "%s\n", cstr);
}

v8::Local<v8::Object> WrapObject(v8::Isolate * isolate, Console * c)
{
    v8::EscapableHandleScope handle_scope(GetIsolate());

    v8::Local<v8::ObjectTemplate> raw_t = v8::ObjectTemplate::New(isolate);

    raw_t->SetInternalFieldCount(1);

    /// Setup log
    raw_t->Set(
        v8::String::NewFromUtf8(GetIsolate(), "log", v8::NewStringType::kNormal).ToLocalChecked(), v8::FunctionTemplate::New(isolate, log));

    /// Setup error
    raw_t->Set(
        v8::String::NewFromUtf8(GetIsolate(), "error", v8::NewStringType::kNormal).ToLocalChecked(),
        v8::FunctionTemplate::New(GetIsolate(), error));

    auto class_t = v8::Local<v8::ObjectTemplate>::New(isolate, raw_t);

    /// create instance
    v8::Local<v8::Object> result = class_t->NewInstance(GetIsolate()->GetCurrentContext()).ToLocalChecked();

    /// create wrapper
    v8::Local<v8::External> ptr = v8::External::New(GetIsolate(), c);
    result->SetInternalField(0, ptr);

    return handle_scope.Escape(result);
}

void Print(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    printf("print called %i\n", args.Length());
    bool first = true;
    for (int i = 0; i < args.Length(); i++)
    {
        v8::HandleScope handle_scope(args.GetIsolate());
        if (first)
        {
            first = false;
        }
        else
        {
            printf(" ");
        }

        v8::String::Utf8Value str(GetIsolate(), args[i]);
        const char * cstr = ToCString(str);
        printf("%s", cstr);
    }
    printf("\n");
    fflush(stdout);
}

void Quit(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    std::exit(0);
}

v8::Local<v8::Object> installConsole(v8::Isolate * isolate)
{
    v8::Isolate::Scope isolate_scope(isolate);
    isolate_ = isolate;

    // Create a stack-allocated handle scope.
    v8::HandleScope handle_scope(isolate);

    // create a template
    v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);

    // use print
    global->Set(
        v8::String::NewFromUtf8(isolate, "quit", v8::NewStringType::kNormal).ToLocalChecked(), v8::FunctionTemplate::New(isolate, Quit));

    // Create a new context.
    v8::Local<v8::Context> context = v8::Context::New(isolate, nullptr, global);

    // Enter the context for compiling and running the hello world script.
    v8::Context::Scope context_scope(context);

    /// create js object
    Console * c = new Console();
    v8::Local<v8::Object> con = WrapObject(isolate, c);

    context->Global()->Set(context, v8::String::NewFromUtf8(isolate, "console", v8::NewStringType::kNormal).ToLocalChecked(), con);
}
