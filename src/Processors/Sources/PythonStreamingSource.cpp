#include <Processors/Sources/PythonStreamingSource.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PythonModuleSession.h>
#include <CPython/Utils.h>
#include <Checkpoint/CheckpointContext.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>

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

    PyObject * method_raw = nullptr;
    int rc = PyObject_GetOptionalAttrString(obj, method_name, &method_raw);
    if (rc <= 0)
    {
        if (rc < 0)
            PyErr_Clear();
        return false;
    }

    DB::cpython::PyObjectPtr method{method_raw};
    if (!method)
        return false;

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
    : ISource(std::move(header), true, getLogger("PythonStreamingSource"), ProcessorID::PythonStreamingSourceID)
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
    /// std::call_once guarantees exactly-once execution even when called
    /// concurrently from generate(), onCancel(), and the destructor.
    std::call_once(finish_once, [&] {
        if (Py_IsInitialized() == 0)
            return;

        auto cleanup = [this] {
            /// Detach the iterator under the lock so a concurrent onCancel() /
            /// generate() never observes a half-reset pointer; the close() call
            /// below runs without the lock held.
            cpython::PyObjectPtr iter_to_close;
            {
                std::lock_guard<std::mutex> lock(py_obj_mutex);
                iter_to_close = std::move(py_iterator);
            }

            if (iter_to_close)
            {
                /// Finalize generators before deinit so hook code observes released iterator state.
                tryCallNoArgMethod(iter_to_close.get(), "close");
                iter_to_close.reset();
            }

            cpython::PythonModuleSession::closeSession(session, /*ignore_exceptions=*/false, /*acquire_gil=*/false);
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
    });
}

void PythonStreamingSource::onCancel() noexcept
{
    /// Signal cancellation first so generate() can observe it immediately
    /// on the next loop iteration — even before we acquire the GIL.
    cancel_requested.store(true, std::memory_order_release);

    if (!Py_IsInitialized())
        return;

    try
    {
        cpython::GILGuard gil_guard;

        /// Take a strong ref under the lock, then drop the lock before calling
        /// into Python so cancellation can still interrupt a blocked iterator.
        cpython::PyObjectPtr iter_local;
        {
            std::lock_guard<std::mutex> lock(py_obj_mutex);
            if (py_iterator)
                iter_local = cpython::PyObjectPtr::borrow(py_iterator.get());
        }

        if (iter_local)
        {
            bool cancelled = tryCallNoArgMethod(iter_local.get(), "cancel");
            cancelled = tryCallNoArgMethod(iter_local.get(), "close") || cancelled;

            /// If the iterator provides no cancellation hook, fall back to
            /// interrupting the executing thread — but ONLY on a GIL build.
            ///
            /// PyThreadState_SetAsyncExc routes by thread id, which is not a
            /// query-ownership token. On a GIL build this is safe: onCancel
            /// holds the GIL, so the worker is pinned at its last Python point
            /// inside THIS query and cannot finish, clear its tid, get recycled
            /// by the pipeline thread pool, and start another query's Python
            /// before we inject. On a free-threaded (cp314t) build the GIL no
            /// longer serializes (GILGuard only attaches a thread state), so
            /// that recycle can race the load()/inject and the KeyboardInterrupt
            /// would land on an unrelated query sharing the recycled
            /// PyThreadState. The branch is compiled out under free-threading;
            /// cancel_requested (checked each generate() iteration) plus the
            /// cancel()/close() hooks above remain the cancellation path there.
            if (!cancelled)
            {
                if constexpr (!cpython::GILGuard::buildSupportsFreeThreading())
                {
                    const auto tid = python_thread_id.load(std::memory_order_acquire);
                    if (tid != 0)
                    {
                        const int set = PyThreadState_SetAsyncExc(tid, PyExc_KeyboardInterrupt);
                        /// If set > 1, the thread ID matched multiple states (should never happen).
                        /// Clear the exception to avoid corrupting unrelated threads.
                        if (set > 1)
                            PyThreadState_SetAsyncExc(tid, nullptr);
                    }
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
    if (exhausted.load(std::memory_order_acquire))
        return {};

    if (isCancelled() || cancel_requested.load(std::memory_order_acquire))
    {
        exhausted.store(true, std::memory_order_release);
        return {};
    }

    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    cpython::GILGuard gil_guard;

    auto this_thread_id = PyThread_get_thread_ident();
    python_thread_id.store(this_thread_id, std::memory_order_release);
    SCOPE_EXIT({
        /// Only clear if the stored ID is still ours — avoids clobbering
        /// a concurrent generate() call's thread ID.
        python_thread_id.compare_exchange_strong(this_thread_id, 0, std::memory_order_release);
        /// Advance the processed SN so the checkpoint barrier is taken; the value
        /// is not persisted/restored — a Python iterator has no resumable
        /// position (see doCheckpoint / doRecover).
        setLastProcessedSN(lastProcessedSN() + 1);
    });

    /// Take a strong ref to the iterator under the lock so a concurrent
    /// finishPython() reset cannot free it mid-iteration; release the lock
    /// before iterNext() so the (possibly blocking) call stays interruptible.
    cpython::PyObjectPtr iter_local;
    {
        std::lock_guard<std::mutex> lock(py_obj_mutex);
        if (py_iterator)
            iter_local = cpython::PyObjectPtr::borrow(py_iterator.get());
    }
    if (!iter_local)
    {
        exhausted.store(true, std::memory_order_release);
        return {};
    }

    const auto & output_header = getPort().getHeader();

    while (true)
    {
        if (isCancelled() || cancel_requested.load(std::memory_order_acquire))
        {
            exhausted.store(true, std::memory_order_release);
            return {};
        }

        cpython::PyObjectPtr next_item;
        try
        {
            next_item = cpython::iterNext(iter_local);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::QUERY_WAS_CANCELLED && (isCancelled() || cancel_requested.load(std::memory_order_acquire)))
            {
                exhausted.store(true, std::memory_order_release);
                return {};
            }

            throw;
        }

        if (!next_item)
        {
            /// Iterator exhausted
            exhausted.store(true, std::memory_order_release);
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

Chunk PythonStreamingSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// A Python iterator cannot be seeked, so there is no resumable position to
    /// persist. Notify the coordinator we have seen this checkpoint epoch (no
    /// state saved) and emit a barrier chunk, mirroring GenerateRandomSource.
    /// doRecover()/doResetStartSN() are no-ops, so the source never advertises a
    /// recovered offset it cannot honor.
    IProcessor::checkpoint(ckpt_ctx_);

    auto result = Chunk{getPort().getHeader().getColumns(), 0};
    result.setCheckpointContext(ckpt_ctx_);
    return result;
}
}

#endif
