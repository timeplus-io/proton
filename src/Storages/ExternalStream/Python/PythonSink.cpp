#include <Storages/ExternalStream/Python/PythonSink.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/Utils.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
}

PythonSink::PythonSink(const Block & header, String python_source_, String function_name_)
    : SinkToStorage(header, ProcessorID::PythonSinkID), python_source(std::move(python_source_)), function_name(std::move(function_name_))
{
    initPython();
}

PythonSink::~PythonSink()
{
    if (Py_IsInitialized() == 0)
        return;

    cpython::GILGuard gil_guard;
    py_function.reset();
    if (!module_name.empty())
        cpython::unloadModule(module_name);
}

void PythonSink::initPython()
{
    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    module_name = getName() + cpython::randomModuleName();

    cpython::GILGuard gil_guard;
    auto byte_code = cpython::compile(python_source);
    cpython::executeByteCode(byte_code, module_name);
    py_function = cpython::getFunction(function_name, module_name);
}

void PythonSink::consume(Chunk chunk)
{
    if (chunk.rows() == 0)
        return;

    cpython::GILGuard gil_guard;

    Block input_block = getHeader().cloneWithColumns(chunk.detachColumns());
    size_t columns = input_block.columns();
    cpython::PyObjectPtr py_args{PyTuple_New(static_cast<Py_ssize_t>(columns))};
    for (size_t i = 0; i < columns; ++i)
    {
        const auto & col_with_type = input_block.getByPosition(i);
        auto py_col = cpython::convertColumnToPythonList(col_with_type);
        PyTuple_SetItem(py_args.get(), static_cast<Py_ssize_t>(i), py_col.release());
    }

    cpython::executeObject(py_function, py_args);
}
}

#endif
