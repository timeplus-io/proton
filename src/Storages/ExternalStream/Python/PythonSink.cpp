#include <Storages/ExternalStream/Python/PythonSink.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PythonModuleSession.h>

namespace DB
{
PythonSink::PythonSink(const Block & header, cpython::PythonFunction function_)
    : SinkToStorage(header, ProcessorID::PythonSinkID), session(cpython::PythonModuleSession::create(getName(), std::move(function_)))
{
}

PythonSink::~PythonSink()
{
    finishPython(/*ignore_exceptions=*/true);
}

void PythonSink::finishPython(bool ignore_exceptions)
{
    cpython::PythonModuleSession::closeSession(session, ignore_exceptions);
}

void PythonSink::onFinish()
{
    finishPython(/*ignore_exceptions=*/false);
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

    session->execute(py_args);
}
}

#endif
