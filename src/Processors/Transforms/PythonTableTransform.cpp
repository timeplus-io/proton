#include <Processors/Transforms/PythonTableTransform.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
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
extern const int UDF_RUNNING_ERROR;
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
    String python_source_,
    String function_name_,
    Names input_column_names_,
    ColumnsDescription output_columns_,
    Names requested_output_columns_,
    PythonTableMode mode_)
    : ISimpleTransform(input_header, std::move(output_header), true, ProcessorID::PythonTableTransformID)
    , requested_output_columns(std::move(requested_output_columns_))
    , output_columns(std::move(output_columns_))
    , tuple_type(buildTupleType(output_columns))
    , mode(mode_)
    , python_source(std::move(python_source_))
    , function_name(std::move(function_name_))
{
    input_positions.reserve(input_column_names_.size());
    for (const auto & name : input_column_names_)
    {
        if (!input_header.has(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column '{}' not found in source for Python external stream", name);
        input_positions.push_back(input_header.getPositionByName(name));
    }

    initPython();
}

PythonTableTransform::~PythonTableTransform()
{
    if (py_function && Py_IsInitialized())
    {
        cpython::GILGuard gil_guard;
        py_function.reset();
        if (!module_name.empty())
            cpython::unloadModule(module_name);
    }
}

void PythonTableTransform::onCancel() noexcept
{
    cancel_requested.store(true, std::memory_order_release);

    if (!Py_IsInitialized())
        return;

    try
    {
        cpython::GILGuard gil_guard;

        const auto thread_id = python_thread_id.load(std::memory_order_acquire);
        if (thread_id == 0)
            return;

        const int set = PyThreadState_SetAsyncExc(thread_id, PyExc_KeyboardInterrupt);
        if (set > 1)
            PyThreadState_SetAsyncExc(thread_id, nullptr);
    }
    catch (...)
    {
        /// no-throw on cancellation path
    }
}

void PythonTableTransform::initPython()
{
    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    module_name = getName() + cpython::randomModuleName();

    cpython::GILGuard gil_guard;
    auto byte_code = cpython::compile(python_source);
    cpython::executeByteCode(byte_code, module_name);
    py_function = cpython::getFunction(function_name, module_name);
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

    cpython::GILGuard gil_guard;

    python_thread_id.store(PyThread_get_thread_ident(), std::memory_order_release);
    SCOPE_EXIT({ python_thread_id.store(0, std::memory_order_release); });

    Block input_block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());
    cpython::PyObjectPtr py_args{PyTuple_New(static_cast<Py_ssize_t>(input_positions.size()))};
    for (size_t i = 0; i < input_positions.size(); ++i)
    {
        const auto & col_with_type = input_block.getByPosition(input_positions[i]);
        auto py_col = cpython::convertColumnToPythonList(col_with_type);
        PyTuple_SetItem(py_args.get(), static_cast<Py_ssize_t>(i), py_col.release());
    }

    auto py_result = cpython::executeObject(py_function, py_args);

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
