#include <Storages/ExternalStream/Python/StoragePythonTable.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/PythonStreamingSource.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/ExternalStream/Python/PythonSink.h>
#include <base/scope_guard.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
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

StoragePythonTable::StoragePythonTable(
    const StorageID & table_id,
    const ColumnsDescription & columns,
    String function_name_,
    String source_code_,
    PythonTableMode mode_,
    String sink_function_name_)
    : IStorage(table_id)
    , function_name(std::move(function_name_))
    , source_code(std::move(source_code_))
    , mode(mode_)
    , sink_function_name(std::move(sink_function_name_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    setInMemoryMetadata(metadata);
}

StoragePtr StoragePythonTable::create(
    const StorageID & table_id,
    const ColumnsDescription & columns,
    String function_name_,
    String source_code_,
    PythonTableMode mode_,
    String sink_function_name_)
{
    return std::shared_ptr<StoragePythonTable>(new StoragePythonTable(
        table_id, columns, std::move(function_name_), std::move(source_code_), mode_, std::move(sink_function_name_)));
}

cpython::PyObjectPtr StoragePythonTable::executePythonAndGetResult(ContextPtr, String & module_name) const
{
    if (Py_IsInitialized() == 0)
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");

    module_name = getName() + cpython::randomModuleName();

    cpython::GILGuard gil_guard;

    auto byte_code = cpython::compile(source_code);
    cpython::executeByteCode(byte_code, module_name);
    auto py_function = cpython::getFunction(function_name, module_name);

    auto py_args = cpython::PyObjectPtr{PyTuple_New(0)};
    return cpython::executeObject(py_function, py_args);
}

Block StoragePythonTable::convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const
{
    auto tuple_type = buildTupleType(getInMemoryMetadataPtr()->getColumns());
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

Pipe StoragePythonTable::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    if (!column_names.empty())
        storage_snapshot->check(column_names);

    auto tuple_type = buildTupleType(getInMemoryMetadataPtr()->getColumns());
    String module_name;

    /// Execute Python and get result
    auto py_result = executePythonAndGetResult(context, module_name);

    /// Determine if we should use streaming mode
    bool use_streaming = false;
    bool result_is_generator = false;
    {
        cpython::GILGuard gil_guard;

        if (cpython::isAsyncGeneratorOrCoroutine(py_result))
        {
            py_result.reset();
            cpython::unloadModule(module_name);
            throw Exception(
                ErrorCodes::UNSUPPORTED,
                "Python external stream does not support coroutine/async generator results. "
                "Return a synchronous iterator (implementing __iter__/__next__) or a list.");
        }

        result_is_generator = cpython::isGenerator(py_result);

        switch (mode)
        {
            case PythonTableMode::Auto:
                /// Auto-detect: if result is a generator/iterator, use streaming
                use_streaming = result_is_generator;
                break;
            case PythonTableMode::Streaming:
                use_streaming = true;
                break;
            case PythonTableMode::Batch:
                use_streaming = false;
                break;
        }

        if (use_streaming && !result_is_generator)
        {
            py_result.reset();
            cpython::unloadModule(module_name);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream streaming mode requires generator result");
        }

        if (!use_streaming && mode == PythonTableMode::Batch && result_is_generator)
        {
            py_result.reset();
            cpython::unloadModule(module_name);
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream batch mode requires list result");
        }
    }

    if (use_streaming)
    {
        /// Streaming mode - create PythonStreamingSource
        /// Build header block for the requested columns
        Block header;
        auto metadata = getInMemoryMetadataPtr();
        for (const auto & name : column_names)
        {
            auto col_desc = metadata->getColumns().get(name);
            header.insert(ColumnWithTypeAndName{col_desc.type->createColumn(), col_desc.type, name});
        }

        return Pipe(std::make_shared<PythonStreamingSource>(std::move(header), std::move(py_result), tuple_type, std::move(module_name)));
    }
    else
    {
        /// Batch mode - convert entire result to block
        Block block;
        {
            cpython::GILGuard gil_guard;
            block = convertPythonResultToBlock(py_result);
            /// Now we can unload the module
            cpython::unloadModule(module_name);
            /// Release the Python object while we still hold the GIL
            /// to avoid calling Py_XDECREF without GIL at function exit
            py_result.reset();
        }

        Block filtered_block;
        for (const auto & name : column_names)
            filtered_block.insert(block.getByName(name));

        const auto row_count = column_names.empty() ? block.rows() : filtered_block.rows();
        Chunk chunk(filtered_block.getColumns(), row_count);
        return Pipe(std::make_shared<SourceFromSingleChunk>(filtered_block.cloneEmpty(), std::move(chunk)));
    }
}

SinkToStoragePtr StoragePythonTable::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    String sink_name = sink_function_name.empty() ? function_name : sink_function_name;
    if (sink_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream sink requires a function name");

    return std::make_shared<PythonSink>(metadata_snapshot->getSampleBlock(), source_code, std::move(sink_name));
}
}

#endif
