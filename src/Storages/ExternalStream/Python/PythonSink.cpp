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

void PythonSink::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    /// Periodically give the Python code a chance to flush data it has buffered so far.
    if (session)
        session->flush();

    IProcessor::checkpoint(std::move(ckpt_ctx));
}

void PythonSink::consume(Chunk chunk)
{
    if (chunk.rows() == 0)
        return;

    const auto & header = getHeader();
    const auto & columns = chunk.getColumns();
    auto columns_size = columns.size();

    cpython::GILGuard gil_guard;
    cpython::PyObjectPtr py_args{PyTuple_New(static_cast<Py_ssize_t>(columns_size))};

    for (size_t i = 0; i < columns_size; ++i)
    {
        const auto & col_with_type = header.getByPosition(i);
        auto py_col = cpython::convertColumnToPythonList(*columns[i], col_with_type.type);
        PyTuple_SetItem(py_args.get(), static_cast<Py_ssize_t>(i), py_col.release());
    }

    session->execute(py_args);
}
}

#endif
