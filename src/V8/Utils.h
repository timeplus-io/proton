#pragma once

#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>
#include <base/getMemoryAmount.h>
#include <base/types.h>

#include <span>
#include <v8.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_COMPILE_ERROR;
}

namespace V8
{
/// convert input columns to corresponding v8 variants in argv,
/// - row_num: the source row to convert, if row_num = -1, means convert all rows
/// - argv: the result v8 variants
std::vector<v8::Local<v8::Value>> prepareArguments(
    v8::Isolate * isolate, const std::span<const DB::UserDefinedFunctionConfiguration::Argument> arguments, const MutableColumns & columns);

/// convert the v8 variant to corresponding DataType and insert into to column
/// - is_result_array: the result is multiple values or single value, normally for UDF and UDA with own emit strategy, it is true
void insertResult(v8::Isolate * isolate, IColumn & to, const DataTypePtr & type, v8::Local<v8::Value> & result, bool is_result_array);

template <typename... Args>
void throwException(v8::Isolate * isolate, v8::TryCatch & try_catch, int code, const std::string & fmt, Args &&... args)
{
    if (try_catch.Exception()->IsNull() && try_catch.Message().IsEmpty())
    {
        throw Exception(fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...), code);
    }
    else
    {
        v8::String::Utf8Value error(isolate, try_catch.Exception());
        throw Exception(code, "{}:{}", fmt::format(fmt::runtime(fmt), std::forward<Args>(args)...), String(*error, error.length()));
    }
}

/// Compile Javascript code and prepare v8 context by call the 'function' in the new context
/// @param func is like: [&](v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)
/// @return the new local v8::Context
void compileSource(
    v8::Isolate * isolate,
    const std::string & func_name,
    const std::string & source,
    std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)> func);

/// Run func in the specified v8 context
inline void run(
    v8::Isolate * isolate,
    const v8::Persistent<v8::Context> & context,
    std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::TryCatch &)> func)
{
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::TryCatch try_catch(isolate);
    try_catch.SetVerbose(true);
    v8::Local<v8::Context> local_ctx = v8::Local<v8::Context>::New(isolate, context);
    v8::Context::Scope context_scope(local_ctx);

    func(isolate, local_ctx, try_catch);
}

void validateAggregationFunctionSource(
    const std::string & func_name, const std::vector<std::string> & required_member_funcs, const std::string & source);

void validateStatelessFunctionSource(const std::string & func_name, const std::string & source);

/// Check v8 heap size and throw exception if exceeds limit
void checkHeapLimit(v8::Isolate * isolate, size_t max_v8_heap_size_in_bytes);

std::string getHeapStatisticsString(v8::HeapStatistics & heap_statistics);
}
}
