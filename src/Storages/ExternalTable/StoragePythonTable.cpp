#include <Storages/ExternalTable/StoragePythonTable.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <base/scope_guard.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
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
    const StorageID & table_id, const ColumnsDescription & columns, String function_name_, String source_code_)
    : IStorage(table_id), function_name(std::move(function_name_)), source_code(std::move(source_code_))
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    setInMemoryMetadata(metadata);
}

StoragePtr
StoragePythonTable::create(const StorageID & table_id, const ColumnsDescription & columns, String function_name_, String source_code_)
{
    return std::shared_ptr<StoragePythonTable>(
        new StoragePythonTable(table_id, columns, std::move(function_name_), std::move(source_code_)));
}

Block StoragePythonTable::executePython(ContextPtr) const
{
    if (Py_IsInitialized() == 0)
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");

    auto tuple_type = buildTupleType(getInMemoryMetadataPtr()->getColumns());
    auto module_name = getName() + cpython::randomModuleName();

    Block res_block;

    cpython::GILGuard gil_guard;
    SCOPE_EXIT({ cpython::unloadModule(module_name); });

    auto byte_code = cpython::compile(source_code);
    cpython::executeByteCode(byte_code, module_name);
    auto py_function = cpython::getFunction(function_name, module_name);

    auto py_args = cpython::PyObjectPtr{PyTuple_New(0)};
    auto py_result = cpython::executeObject(py_function, py_args);

    auto result_column = cpython::convertPythonListToColumn(py_result, tuple_type);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*result_column);
    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());

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
    storage_snapshot->check(column_names);

    auto block = executePython(context);

    Block filtered_block;
    for (const auto & name : column_names)
        filtered_block.insert(block.getByName(name));

    Chunk chunk(filtered_block.getColumns(), filtered_block.rows());
    return Pipe(std::make_shared<SourceFromSingleChunk>(filtered_block.cloneEmpty(), std::move(chunk)));
}
}

#endif
