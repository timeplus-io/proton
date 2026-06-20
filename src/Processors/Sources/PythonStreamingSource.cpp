#include <Processors/Sources/PythonStreamingSource.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PythonModuleSession.h>
#include <CPython/Utils.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>

#include <base/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
extern const int UDF_RUNNING_ERROR;
}

namespace
{
/// Best-effort cancellation hook. Connector iterators can implement cancel()/close() to
/// release blocking IO and terminate quickly on query cancellation.
bool tryCallNoArgMethod(PyObject * obj, const char * method_name)
{
    if (!obj || !method_name)
        return false;

    if (!PyObject_HasAttrString(obj, method_name))
        return false;

    DB::cpython::PyObjectPtr method{PyObject_GetAttrString(obj, method_name)};
    if (!method)
    {
        PyErr_Clear();
        return false;
    }

    DB::cpython::PyObjectPtr result{PyObject_CallObject(method.get(), nullptr)};
    if (!result)
    {
        PyErr_Clear();
        return false;
    }

    return true;
}
}

PythonStreamingSource::PythonStreamingSource(
    Block header, cpython::PyObjectPtr py_iterator_, DataTypePtr tuple_type_, cpython::PythonModuleSessionPtr session_)
    : ISource(std::move(header), true, ProcessorID::PythonStreamingSourceID)
    , py_iterator(std::move(py_iterator_))
    , tuple_type(std::move(tuple_type_))
    , session(std::move(session_))
{
    init();
}

void PythonStreamingSource::init()
{
    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    const auto & output_header = getPort().getHeader();

    needs_projection_pushdown = output_header.columns() < tuple_type_ptr->getElements().size();
    output_tuple_positions.reserve(output_header.columns());

    for (const auto & output_col : output_header)
    {
        const auto pos = tuple_type_ptr->tryGetPositionByName(output_col.name);
        if (!pos)
            throw Exception(
                ErrorCodes::UDF_RUNNING_ERROR,
                "PythonStreamingSource header column '{}' not found in tuple type '{}'",
                output_col.name,
                tuple_type_ptr->getName());
        output_tuple_positions.emplace_back(*pos);
    }
}

PythonStreamingSource::~PythonStreamingSource()
{
    finishPython(/*ignore_exceptions=*/true);
}

void PythonStreamingSource::finishPython(bool ignore_exceptions, bool acquire_gil)
{
    if (python_finished)
        return;

    if (Py_IsInitialized() == 0)
    {
        python_finished = true;
        return;
    }

    auto cleanup = [this] {
        if (py_iterator)
        {
            /// Finalize generators before deinit so hook code observes released iterator state.
            tryCallNoArgMethod(py_iterator.get(), "close");
            py_iterator.reset();
        }

        cpython::PythonModuleSession::closeSession(session, /*ignore_exceptions=*/false, /*acquire_gil=*/false);
        python_finished = true;
    };

    auto runWithGil = [&] {
        if (acquire_gil)
        {
            cpython::GILGuard gil_guard;
            cleanup();
        }
        else
        {
            cleanup();
        }
    };

    if (ignore_exceptions)
    {
        try
        {
            runWithGil();
        }
        catch (...)
        {
        }
    }
    else
    {
        runWithGil();
    }
}

void PythonStreamingSource::onCancel() noexcept
{
    cancel_requested.store(true, std::memory_order_release);

    if (!Py_IsInitialized())
        return;

    try
    {
        cpython::GILGuard gil_guard;

        if (py_iterator)
        {
            bool cancelled = tryCallNoArgMethod(py_iterator.get(), "cancel");
            cancelled = tryCallNoArgMethod(py_iterator.get(), "close") || cancelled;

            /// If the iterator doesn't provide a cancellation hook, try to interrupt the executing thread.
            if (!cancelled)
            {
                const auto thread_id = python_thread_id.load(std::memory_order_acquire);
                if (thread_id != 0)
                {
                    const int set = PyThreadState_SetAsyncExc(thread_id, PyExc_KeyboardInterrupt);
                    if (set > 1)
                        PyThreadState_SetAsyncExc(thread_id, nullptr);
                }
            }
        }
    }
    catch (...)
    {
        /// no-throw on cancellation path
    }
}

Block PythonStreamingSource::convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const
{
    Block res_block;

    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    auto normalized = cpython::normalizePythonListForTuple(py_result, tuple_type_ptr->getElements().size());
    auto result_column = cpython::convertPythonListToColumn(normalized, tuple_type);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*result_column);

    size_t elements = tuple_type_ptr->getElements().size();
    for (size_t i = 0; i < elements; ++i)
    {
        const auto & column = tuple_column.getColumnPtr(i);
        const auto & type = tuple_type_ptr->getElement(i);
        auto element_name = tuple_type_ptr->getNameByPosition(i + 1);
        res_block.insert(ColumnWithTypeAndName{column, type, element_name});
    }

    return res_block;
}

Block PythonStreamingSource::convertPythonResultToOutputBlock(const cpython::PyObjectPtr & py_result) const
{
    Block res_block;

    const auto & output_header = getPort().getHeader();
    if (output_header.columns() == 0)
        return res_block;

    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    const size_t expected_row_size = tuple_type_ptr->getElements().size();
    auto normalized = cpython::normalizePythonListForTuple(py_result, tuple_type_ptr->getElements().size());

    if (!normalized)
        return res_block;

    if (!PyList_Check(normalized.get()))
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "PythonStreamingSource expected a list of rows from Python generator, got {}",
            cpython::getObjectType(normalized));

    const Py_ssize_t rows = PyList_Size(normalized.get());
    if (rows <= 0)
        return res_block;

    for (size_t out_idx = 0; out_idx < output_header.columns(); ++out_idx)
    {
        const auto tuple_pos = output_tuple_positions.at(out_idx);
        const auto & output_col = output_header.getByPosition(out_idx);

        cpython::PyObjectPtr values{PyList_New(rows)};
        if (!values)
            throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to allocate Python list for projected column conversion");

        for (Py_ssize_t row_idx = 0; row_idx < rows; ++row_idx)
        {
            PyObject * row = PyList_GetItem(normalized.get(), row_idx);
            if (!row)
                throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to access row {} from Python generator output", row_idx);

            const bool is_tuple = PyTuple_Check(row);
            const bool is_list = PyList_Check(row);
            if (!is_tuple && !is_list)
                throw Exception(
                    ErrorCodes::UDF_RUNNING_ERROR,
                    "PythonStreamingSource expected each row to be tuple/list, got {}",
                    cpython::getObjectType(cpython::PyObjectPtr::borrow(row)));

            const Py_ssize_t row_size = is_tuple ? PyTuple_Size(row) : PyList_Size(row);
            if (row_size < 0)
            {
                if (cpython::hasException())
                    throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read row size: {}", cpython::getExceptionMessage());
                throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read row size (negative)");
            }

            if (static_cast<size_t>(row_size) != expected_row_size)
                throw Exception(
                    ErrorCodes::UDF_RUNNING_ERROR,
                    "PythonStreamingSource row {} has wrong arity: expected {}, got {}",
                    row_idx,
                    expected_row_size,
                    static_cast<size_t>(row_size));

            PyObject * item = nullptr;
            if (is_tuple)
                item = PyTuple_GetItem(row, tuple_pos);
            else
                item = PyList_GetItem(row, tuple_pos);

            if (!item)
            {
                if (cpython::hasException())
                    throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read projected value: {}", cpython::getExceptionMessage());
                throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read projected value (null)");
            }

            Py_INCREF(item);
            PyList_SET_ITEM(values.get(), row_idx, item);
        }

        auto column = cpython::convertPythonListToColumn(values, output_col.type);
        res_block.insert(ColumnWithTypeAndName{std::move(column), output_col.type, output_col.name});
    }

    return res_block;
}

Chunk PythonStreamingSource::generate()
{
    if (exhausted)
        return {};

    if (isCancelled() || cancel_requested.load(std::memory_order_acquire))
    {
        exhausted = true;
        return {};
    }

    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    cpython::GILGuard gil_guard;

    python_thread_id.store(PyThread_get_thread_ident(), std::memory_order_release);
    SCOPE_EXIT({ python_thread_id.store(0, std::memory_order_release); });

    const auto & output_header = getPort().getHeader();

    while (true)
    {
        if (isCancelled() || cancel_requested.load(std::memory_order_acquire))
        {
            exhausted = true;
            return {};
        }

        cpython::PyObjectPtr next_item;
        try
        {
            next_item = cpython::iterNext(py_iterator);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED && (isCancelled() || cancel_requested.load(std::memory_order_acquire)))
            {
                exhausted = true;
                return {};
            }

            throw;
        }

        if (!next_item)
        {
            /// Iterator exhausted
            exhausted = true;
            finishPython(/*ignore_exceptions=*/false, /*acquire_gil=*/false);
            return {};
        }

        if (output_header.columns() == 0)
        {
            const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
            const size_t expected_row_size = tuple_type_ptr->getElements().size();
            auto normalized = cpython::normalizePythonListForTuple(next_item, expected_row_size);

            /// Skip empty batches rather than terminating the stream.
            if (!normalized)
                continue;

            if (!PyList_Check(normalized.get()))
                throw Exception(
                    ErrorCodes::UDF_RUNNING_ERROR,
                    "PythonStreamingSource expected a list of rows from Python generator, got {}",
                    cpython::getObjectType(normalized));

            const Py_ssize_t rows = PyList_Size(normalized.get());
            if (rows <= 0)
                continue;

            for (Py_ssize_t row_idx = 0; row_idx < rows; ++row_idx)
            {
                PyObject * row = PyList_GetItem(normalized.get(), row_idx);
                if (!row)
                    throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to access row {} from Python generator output", row_idx);

                const bool is_tuple = PyTuple_Check(row);
                const bool is_list = PyList_Check(row);
                if (!is_tuple && !is_list)
                    throw Exception(
                        ErrorCodes::UDF_RUNNING_ERROR,
                        "PythonStreamingSource expected each row to be tuple/list, got {}",
                        cpython::getObjectType(cpython::PyObjectPtr::borrow(row)));

                const Py_ssize_t row_size = is_tuple ? PyTuple_Size(row) : PyList_Size(row);
                if (row_size < 0)
                {
                    if (cpython::hasException())
                        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read row size: {}", cpython::getExceptionMessage());
                    throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read row size (negative)");
                }

                if (static_cast<size_t>(row_size) != expected_row_size)
                    throw Exception(
                        ErrorCodes::UDF_RUNNING_ERROR,
                        "PythonStreamingSource row {} has wrong arity: expected {}, got {}",
                        row_idx,
                        expected_row_size,
                        static_cast<size_t>(row_size));
            }

            return Chunk(Columns{}, static_cast<UInt64>(rows));
        }

        /// Convert the yielded Python object to a Block.
        /// The yielded item should be a list of tuples (batch of rows).
        auto block = needs_projection_pushdown ? convertPythonResultToOutputBlock(next_item) : convertPythonResultToBlock(next_item);

        if (block.rows() == 0)
            continue;

        /// StoragePythonTable may request only a subset of columns (projection pushdown).
        /// Python generator still yields full rows, so we must align the emitted chunk with OutputPort header.
        Columns output_columns;
        output_columns.reserve(output_header.columns());
        for (const auto & header_col : output_header)
            output_columns.emplace_back(block.getByName(header_col.name).column);

        return Chunk(std::move(output_columns), block.rows());
    }
}
}

#endif
