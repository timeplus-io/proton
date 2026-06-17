#include <Functions/UserDefined/PythonUserDefinedFunction.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/Utils.h>
#include <CPython/validatePython.h>
#include <Columns/IColumn.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Common/assert_cast.h>

#if USE_NUMPY
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UDF_RUNNING_ERROR;
extern const int UNSUPPORTED;
}

PythonUserDefinedFunction::PythonUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_)
    : UserDefinedFunctionBase(std::move(udf_desc_), std::move(context_), "PythonUserDefinedFunction")
{
#if !USE_NUMPY
    if (using_numpy)
        throw Exception(ErrorCodes::UNSUPPORTED, "Numpy is not enabled, because Proton is not compiled with numpy library.");
#endif

    arg_num = udf_desc->arguments.size();
    if (!Py_IsInitialized())
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");

    module_name = cpython::randomModuleName();
}

PythonUserDefinedFunction::~PythonUserDefinedFunction()
{
    if (py_function && Py_IsInitialized())
    {
        cpython::GILGuard gil_guard;
        py_function.reset();
        cpython::unloadModule(module_name);
    }
}

void PythonUserDefinedFunction::init() const
{
    const auto & python_udf_payload = assert_cast<const cluster::protocol::PythonUserDefinedFunctionPayload &>(*udf_desc->payload);

    cpython::GILGuard gil_guard;
    const auto & udf_name = udf_desc->name;
    /// check if already exists, it not we register the function in python namespace, every function will only be registered once.
    auto main_module = cpython::getOrAddModule(module_name);
    py_function = cpython::tryGetAttr(main_module, udf_name);
    if (!py_function)
    {
        try
        {
            /// load function
            auto byte_code = cpython::compile(python_udf_payload.source);
            cpython::executeByteCode(byte_code, module_name);
            Streaming::runPythonUDFInitHook(python_udf_payload, udf_name, module_name);
        }
        catch (...)
        {
            /// Unload the half-initialized module so the call_once retry reloads
            /// it and re-runs the init hook instead of finding it "already loaded".
            py_function.reset();
            if (cpython::hasModule(module_name))
                cpython::unloadModule(module_name);
            throw;
        }
    }

    /// get the function object by UDF name
    py_function = cpython::getFunction(udf_name, module_name);
}

ColumnPtr PythonUserDefinedFunction::userDefinedExecuteImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    std::call_once(init_flag, [this]() { this->init(); });

    cpython::GILGuard gil_guard;
    chassert(py_function);

    /// step 1: convert the column to python data type
    cpython::PyObjectPtr py_args{PyTuple_New(arg_num)};
    for (size_t i = 0; i < arg_num; i++)
    {
#if USE_NUMPY
        /// if numpy enabled, it depends on the using_numpy flag, if using_numpy, we convert the column to numpy array,else convert to python list
        if (using_numpy)
            auto py_arg = cpython::convertColumnToNumpyArray(*arguments[i].column);
        else
            auto py_arg = cpython::convertColumnToPythonList(*arguments[i].column, arguments[i].type);
#else
        /// if not using numpy, we convert the column to python list
        auto py_arg = cpython::convertColumnToPythonList(*arguments[i].column, arguments[i].type);
#endif
        PyTuple_SetItem(py_args.get(), i, py_arg.release());
    }

    /// step 2: call the python function with the arguments
    auto py_result = cpython::executeObject(py_function, py_args);

    /// step 3: convert the python data to column
#if USE_NUMPY
    ColumnPtr column = nullptr;
    if (using_numpy)
        column = cpython::covertNumpyArrayToColumn(py_result, result_type);
    else
        column = cpython::convertPythonListToColumn(py_result, result_type);
#else
    auto column = cpython::convertPythonListToColumn(py_result, result_type);
#endif

    return column;
}
}

#endif /// USE_PYTHON_UDF
