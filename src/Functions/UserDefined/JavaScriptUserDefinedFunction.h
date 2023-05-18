#pragma once

#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>

#include <v8.h>

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

    explicit JavaScriptExecutionContext(const JavaScriptUserDefinedFunctionConfiguration & config, ContextPtr ctx);

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
    explicit JavaScriptUserDefinedFunction(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_);

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

private:
    std::unique_ptr<JavaScriptExecutionContext> js_ctx = nullptr;
};

}
