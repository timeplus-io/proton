#pragma once

#include "config.h"

#if USE_V8

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Functions/UserDefined/UserDefinedFunctionBase.h>

#include <V8/V8Includes.h>

namespace DB
{

/// JS UDF Execution Context, holds the v8::isolate and JSUserDefinedFunction, which will be dispose when a query ends
class JavaScriptExecutionContext
{
public:
    struct IsolateDeleter
    {
    public:
        void operator()(v8::Isolate * isolate_) const { isolate_->Dispose(); }
    };

    explicit JavaScriptExecutionContext(const String & name, const String & source, ContextPtr ctx, LoggerPtr logger);

    std::unique_ptr<v8::Isolate, JavaScriptExecutionContext::IsolateDeleter> isolate;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Function> func;

    ~JavaScriptExecutionContext()
    {
        func.Reset();
        context.Reset();
    }
};

class JavaScriptUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    explicit JavaScriptUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_);

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

private:
    using JavaScriptExecutionContextPtr = std::unique_ptr<JavaScriptExecutionContext>;
    std::vector<JavaScriptExecutionContextPtr> js_ctxes;
    mutable std::atomic<size_t> js_ctx_idx = 0;
};

}

#endif
