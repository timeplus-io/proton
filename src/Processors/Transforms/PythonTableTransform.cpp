#include <Processors/Transforms/PythonTableTransform.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PythonModuleSession.h>
#include <CPython/Utils.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <base/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED;
}

namespace
{
DataTypePtr buildTupleType(const ColumnsDescription & columns)
{
    DataTypes element_types;
    Strings element_names;
    element_types.reserve(columns.getAll().size());
    element_names.reserve(columns.getAll().size());

    for (const auto & column : columns.getAll())
    {
        element_types.emplace_back(column.type);
        element_names.emplace_back(column.name);
    }

    return std::make_shared<DataTypeTuple>(element_types, element_names);
}
}

PythonTableTransform::PythonTableTransform(
    const Block & input_header,
    Block output_header,
    Names input_column_names_,
    ColumnsDescription output_columns_,
    Names requested_output_columns_,
    PythonTableMode mode_,
    cpython::PythonModuleSessionPtr session_,
    PythonTableTransformSharedStatePtr shared_state_)
    : ISimpleTransform(input_header, std::move(output_header), true, ProcessorID::PythonTableTransformID)
    , requested_output_columns(std::move(requested_output_columns_))
    , output_columns(std::move(output_columns_))
    , tuple_type(buildTupleType(output_columns))
    , mode(mode_)
    , session(std::move(session_))
    , shared_state(std::move(shared_state_))
{
    input_positions.reserve(input_column_names_.size());
    for (const auto & name : input_column_names_)
    {
        if (!input_header.has(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in source for Python external stream", name);
        input_positions.push_back(input_header.getPositionByName(name));
    }

    if (shared_state)
        shared_state->retain();
}

PythonTableTransform::~PythonTableTransform()
{
    if (shared_state)
    {
        if (!finalization_completed)
        {
            /// work() never ran our finalization branch (abnormal exit).
            /// Release our share so siblings can correctly identify the
            /// last live owner; do not run the close here — the session's
            /// own destructor (fires when the last shared_ptr is released)
            /// is the safety net for the abort path, with
            /// ignore_exceptions=true.
            shared_state->live_count.fetch_sub(1, std::memory_order_acq_rel);
        }
        return;
    }

    /// Single-owner case (no shared state). If work() did not finalize,
    /// run a best-effort close so deinit still gets a chance.
    finishSession(/*ignore_exceptions=*/true);
}

void PythonTableTransform::finishSession(bool ignore_exceptions)
{
    std::call_once(finish_once, [&] {
        cpython::PythonModuleSession::closeSession(session, ignore_exceptions);
    });
}

PythonTableTransform::Status PythonTableTransform::prepare()
{
    if (finalization_completed)
        return Status::Finished;

    if (finalization_pending)
        return Status::Ready;

    const bool input_finished_before = !has_input && input.isFinished();
    const bool output_finished_before = output.isFinished();

    auto status = ISimpleTransform::prepare();
    if (status == Status::Finished)
    {
        /// Normal completion: input drained and downstream still has room
        /// for the close-side effects (this is the only state in which we
        /// want deinit exceptions to propagate). Otherwise (abort, broken
        /// pipe, etc.) swallow exceptions to avoid masking the original
        /// failure.
        finalization_ignore_exceptions = !(input_finished_before && !output_finished_before);
        finalization_pending = true;
        return Status::Ready;
    }

    return status;
}

void PythonTableTransform::work()
{
    if (finalization_pending)
    {
        /// In the parallel-pipeline case (shared_state != nullptr), only the
        /// last live owner runs the explicit close. Earlier finalizers just
        /// drop their share; the session stays open for siblings still in
        /// transform(). The single-owner case (shared_state == nullptr,
        /// e.g. unit tests) is always "last" so close runs unconditionally
        /// with the finalization_ignore_exceptions semantics derived in
        /// prepare().
        const bool we_are_last = !shared_state || shared_state->releaseAndClaimLast();
        if (we_are_last)
            finishSession(finalization_ignore_exceptions);

        finalization_pending = false;
        finalization_completed = true;
        return;
    }

    ISimpleTransform::work();
}

void PythonTableTransform::onCancel() noexcept
{
    cancel_requested.store(true, std::memory_order_release);

    /// Fall back to interrupting the executing thread — but ONLY on a GIL
    /// build. PyThreadState_SetAsyncExc routes by thread id, which is a valid
    /// query-ownership token only while the GIL is held: onCancel holds it, so
    /// the worker is pinned in THIS query and cannot finish, clear its tid, be
    /// recycled by the pipeline thread pool, and start another query's Python
    /// before we inject. On a free-threaded (cp314t) build the GIL no longer
    /// serializes (GILGuard only attaches a thread state), so that recycle can
    /// race the load()/inject and the KeyboardInterrupt would land on an
    /// unrelated query sharing the recycled PyThreadState. Compiled out under
    /// free-threading; cancel_requested (checked on each transform() entry) is
    /// the cancellation path there.
    if constexpr (!cpython::GILGuard::buildSupportsFreeThreading())
    {
        if (!Py_IsInitialized())
            return;

        try
        {
            cpython::GILGuard gil_guard;

            const auto tid = python_thread_id.load(std::memory_order_acquire);
            if (tid == 0)
                return;

            const int set = PyThreadState_SetAsyncExc(tid, PyExc_KeyboardInterrupt);
            if (set > 1)
                PyThreadState_SetAsyncExc(tid, nullptr);
        }
        catch (...)
        {
            /// no-throw on cancellation path
        }
    }
}

Block PythonTableTransform::convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const
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

Block PythonTableTransform::filterOutputBlock(Block && block) const
{
    if (requested_output_columns.empty())
        return {};

    Block filtered_block;
    filtered_block.reserve(requested_output_columns.size());
    for (const auto & name : requested_output_columns)
        filtered_block.insert(block.getByName(name));

    return filtered_block;
}

void PythonTableTransform::transform(Chunk & chunk)
{
    if (chunk.rows() == 0)
    {
        /// Ensure empty chunks still match the output header
        chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
        return;
    }

    if (isCancelled() || cancel_requested.load(std::memory_order_acquire))
    {
        chunk.setColumns(getOutputPort().getHeader().cloneEmptyColumns(), 0);
        return;
    }

    const auto & input_header = getInputPort().getHeader();
    const auto & columns = chunk.getColumns();

    cpython::GILGuard gil_guard;

    auto this_thread_id = PyThread_get_thread_ident();
    python_thread_id.store(this_thread_id, std::memory_order_release);
    SCOPE_EXIT({
        python_thread_id.compare_exchange_strong(this_thread_id, 0, std::memory_order_release);
    });

    cpython::PyObjectPtr py_args{PyTuple_New(static_cast<Py_ssize_t>(input_positions.size()))};
    for (size_t i = 0; i < input_positions.size(); ++i)
    {
        const auto & col_with_type = input_header.getByPosition(input_positions[i]);
        auto py_col = cpython::convertColumnToPythonList(*columns[input_positions[i]], col_with_type.type);
        PyTuple_SetItem(py_args.get(), static_cast<Py_ssize_t>(i), py_col.release());
    }

    auto py_result = session->execute(py_args);

    if (cpython::isAsyncGeneratorOrCoroutine(py_result))
        throw Exception(
            ErrorCodes::UNSUPPORTED,
            "Python external stream does not support coroutine/async generator results. "
            "Return a synchronous iterator (implementing __iter__/__next__) or a list.");

    const bool result_is_generator = cpython::isGenerator(py_result);
    if (mode == PythonTableMode::Streaming && !result_is_generator)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream streaming mode requires generator result");
    if (mode == PythonTableMode::Batch && result_is_generator)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream batch mode requires list result");

    const bool use_generator = result_is_generator && mode != PythonTableMode::Batch;

    Block res_block;
    if (use_generator)
    {
        MutableColumns result_columns;
        const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
        size_t num_cols = tuple_type_ptr->getElements().size();
        result_columns.reserve(num_cols);

        for (size_t i = 0; i < num_cols; ++i)
            result_columns.push_back(tuple_type_ptr->getElement(i)->createColumn());

        while (auto next_item = cpython::iterNext(py_result))
        {
            auto normalized = cpython::normalizePythonListForTuple(next_item, num_cols);
            auto result_column = cpython::convertPythonListToColumn(normalized, tuple_type);
            const auto & tuple_column = assert_cast<const ColumnTuple &>(*result_column);

            for (size_t i = 0; i < num_cols; ++i)
            {
                const auto & col = tuple_column.getColumnPtr(i);
                result_columns[i]->insertRangeFrom(*col, 0, col->size());
            }
        }

        for (size_t i = 0; i < num_cols; ++i)
        {
            const auto & type = tuple_type_ptr->getElement(i);
            auto element_name = tuple_type_ptr->getNameByPosition(i + 1);
            res_block.insert(ColumnWithTypeAndName{std::move(result_columns[i]), type, element_name});
        }
    }
    else
    {
        res_block = convertPythonResultToBlock(py_result);
    }

    const auto result_rows = res_block.rows();
    auto filtered_block = filterOutputBlock(std::move(res_block));
    const auto output_rows = requested_output_columns.empty() ? result_rows : filtered_block.rows();
    chunk.setColumns(filtered_block.getColumns(), output_rows);
}
}

#endif
