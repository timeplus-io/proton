#include "config.h"

#if USE_V8

#include <Functions/UserDefined/JavaScriptUserDefinedFunction.h>

#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
#include <base/getMemoryAmount.h>

#include <span>

namespace DB
{

namespace ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_COMPILE_ERROR;
}

JavaScriptExecutionContext::JavaScriptExecutionContext(const String & name, const String & source, ContextPtr ctx, LoggerPtr logger)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());

    size_t max_heap_size_in_bytes = static_cast<size_t>(getMemoryAmountOrZeroCached() * 0.2);
    isolate_params.constraints.ConfigureDefaultsFromHeapSize(0, max_heap_size_in_bytes);
    isolate_params.constraints.set_max_old_generation_size_in_bytes(max_heap_size_in_bytes);

    isolate = std::unique_ptr<v8::Isolate, IsolateDeleter>(v8::Isolate::New(isolate_params), IsolateDeleter());

    isolate.get()->AddNearHeapLimitCallback(V8::nearHeapLimitCallback, isolate.get());
    isolate.get()->SetFatalErrorHandler(V8::fatalErrorHandle);

    /// check heap limit
    V8::checkHeapLimit(isolate.get(), ctx->getSettingsRef().javascript_max_memory_bytes, ctx->getSettingsRef().v8_log_interval_ms, logger);

    auto init_functions = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx_, [[maybe_unused]] v8::TryCatch & try_catch, [[maybe_unused]] v8::Local<v8::Value> &) {
        v8::Local<v8::Value> function_val;
        if (!ctx_->Global()->Get(ctx_, V8::to_v8(isolate_, name)).ToLocal(&function_val) || !function_val->IsFunction())
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", name);

        func.Reset(isolate_, function_val.As<v8::Function>());

        context.Reset(isolate_, ctx_);
    };

    V8::compileSource(isolate.get(), name, source, init_functions);
}

JavaScriptUserDefinedFunction::JavaScriptUserDefinedFunction(
    cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_)
    : UserDefinedFunctionBase(std::move(udf_desc_), std::move(context_), "JavaScriptUserDefinedFunction")
{
    auto javascript_udf_payload = std::dynamic_pointer_cast<cluster::protocol::JavaScriptUserDefinedFunctionPayload>(udf_desc->payload);

    auto vms = std::min<size_t>(32, context->getSettingsRef().javascript_vms.value);
    js_ctxes.reserve(vms);
    for (size_t i = 0; i < vms; ++i)
        js_ctxes.push_back(std::make_unique<JavaScriptExecutionContext>(udf_desc->name, javascript_udf_payload->source, context, logger));
}

ColumnPtr JavaScriptUserDefinedFunction::userDefinedExecuteImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    auto javascript_udf_payload
        = std::dynamic_pointer_cast<const cluster::protocol::JavaScriptUserDefinedFunctionPayload>(udf_desc->payload);

    if (arguments.size() < 1)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Empty input argument is not supported for Javascript UDF.");

    assert(arguments[0].column);
    size_t row_num = arguments[0].column->size();
    MutableColumns columns;
    for (const auto & arg : arguments)
        columns.emplace_back(IColumn::mutate(arg.column));

    auto result_column = result_type->createColumn();
    result_column->reserve(row_num);

    const auto & js_ctx = js_ctxes[js_ctx_idx++ % js_ctxes.size()];
    auto execute = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        /// First, get local function of UDF
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, js_ctx->func);

        /// Second, convert the input column into the corresponding object used by UDF
        auto argv = V8::prepareArguments(isolate_, std::span(udf_desc->arguments), columns);

        /// Third, execute the UDF and get result
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, ctx->Global(), static_cast<int>(udf_desc->arguments.size()), argv.data()).ToLocal(&res))
            V8::throwException(isolate_, try_catch, ErrorCodes::UDF_INTERNAL_ERROR, "call JavaScript UDF '{}' failed", udf_desc->name);

        /// Forth, insert the result to result_column
        V8::insertResult(isolate_, *result_column, result_type, res, true);
    };

    V8::run(js_ctx->isolate.get(), js_ctx->context, execute);
    return result_column;
}

}

#endif // USE_V8
