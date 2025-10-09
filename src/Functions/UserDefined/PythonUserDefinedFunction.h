#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PyObjectPtr.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Functions/UserDefined/UserDefinedFunctionBase.h>

#include <Python.h>

#include <memory>

namespace DB
{
class PythonUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    PythonUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc, ContextPtr context_);

    ~PythonUserDefinedFunction() override;
    void init() const;

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

private:
    mutable cpython::PyObjectPtr py_function;
    std::string module_name;

    /// number of the UDF arguments
    size_t arg_num = 0;

    mutable std::once_flag init_flag;

    bool using_numpy = false;
};
}
#endif
