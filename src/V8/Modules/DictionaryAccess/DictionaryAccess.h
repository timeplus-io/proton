#pragma once

#include <Interpreters/Context.h>

#include <V8/V8Includes.h>


namespace DB::V8
{

/// Module interface for V8 dictionary access functionality
class DictionaryAccess
{
public:
    static DictionaryAccess & instance();

    /// Initialize with context
    void initialize(ContextMutablePtr context);

    /// Install JS bindings to a V8 context
    static void installJS(v8::Isolate * isolate, v8::Local<v8::Context> & context);

    /// Run benchmark code
    static void runBenchmark(size_t operation_count = 100);

private:
    DictionaryAccess() = default;

    /// V8 callback functions
    static void getCardinality(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void getValue(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void setValue(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void batchGetValues(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void batchSetValues(const v8::FunctionCallbackInfo<v8::Value> & args);
    static void log(const v8::FunctionCallbackInfo<v8::Value> & args);

    ContextMutablePtr global_context;
};

}
