#include <Functions/UserDefined/JavaScriptUserDefinedFunction.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_COMPILE_ERROR;
}

JavaScriptExecutionContext::JavaScriptExecutionContext(const JavaScriptUserDefinedFunctionConfiguration & config, ContextPtr ctx)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    isolate = std::unique_ptr<v8::Isolate, IsolateDeleter>(v8::Isolate::New(isolate_params), IsolateDeleter());

    /// check heap limit
    V8::checkHeapLimit(isolate.get(), ctx->getSettingsRef().javascript_max_memory_bytes);

    auto init_functions = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> &) {
        v8::Local<v8::Value> function_val;
        if (!ctx->Global()->Get(ctx, V8::to_v8(isolate_, config.name)).ToLocal(&function_val) || !function_val->IsFunction())
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", config.name);

        func.Reset(isolate_, function_val.As<v8::Function>());

        context.Reset(isolate_, ctx);
    };

    V8::compileSource(isolate.get(), config.name, config.source, init_functions);
}

JavaScriptUserDefinedFunction::JavaScriptUserDefinedFunction(
    ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_)
    : UserDefinedFunctionBase(executable_function_, context_, "JavaScriptUserDefinedFunction")
{
    const auto & config = executable_function->getConfiguration()->as<const JavaScriptUserDefinedFunctionConfiguration &>();
    js_ctx = std::make_unique<JavaScriptExecutionContext>(config, context_);
}

ColumnPtr JavaScriptUserDefinedFunction::userDefinedExecuteImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    const auto * config = executable_function->getConfiguration()->as<const JavaScriptUserDefinedFunctionConfiguration>();

    if (arguments.size() < 1)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Empty input argument is not supported for Javascript UDF.");

    assert(arguments[0].column);
    size_t row_num = arguments[0].column->size();
    MutableColumns columns;
    for (const auto & arg : arguments)
        columns.emplace_back(IColumn::mutate(arg.column));

    auto result_column = result_type->createColumn();
    result_column->reserve(row_num);

    auto execute = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        /// First, get local function of UDF
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, js_ctx->func);

        /// Second, convert the input column into the corresponding object used by UDF
        auto argv = V8::prepareArguments(isolate_, config->arguments, columns);

        /// Third, execute the UDF and get result
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, ctx->Global(), static_cast<int>(config->arguments.size()), argv.data()).ToLocal(&res))
            V8::throwException(isolate_, try_catch, ErrorCodes::UDF_INTERNAL_ERROR, "call JavaScript UDF '{}' failed", config->name);

        /// Forth, insert the result to result_column
        V8::insertResult(isolate_, *result_column, result_type, res, true);
    };

    V8::run(js_ctx->isolate.get(), js_ctx->context, execute);
    return result_column;
}

}
