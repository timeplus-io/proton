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
/// Stateless Python UDF.
///
/// Thread-safety contract:
///   - init() runs at most once (guarded by std::call_once) and acquires
///     the Python runtime scope internally.
///   - userDefinedExecuteImpl() may be called concurrently from multiple
///     query threads.  Each call acquires its own GILGuard.  In GIL-enabled
///     builds the GIL serialises execution; in free-threaded builds
///     concurrent Python function calls are safe per CPython's contract.
///   - The destructor acquires the GIL to release py_function and unload
///     the module.  It assumes no concurrent execute calls are in flight.
///     The engine's function registry ensures this by dropping functions
///     only when no queries hold references.
class PythonUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    PythonUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc, ContextPtr context_);

    ~PythonUserDefinedFunction() override;
    void init() const;

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

private:
    /// Lazily initialised by init() via std::call_once.
    mutable cpython::PyObjectPtr py_function;
    std::string module_name;

    size_t arg_num = 0;

    mutable std::once_flag init_flag;

    bool using_numpy = false;
};
}
#endif
