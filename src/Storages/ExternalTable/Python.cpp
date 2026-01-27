#include <Storages/ExternalTable/Python.h>

#if USE_PYTHON_UDF

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/ExternalTable/StoragePythonTable.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace ExternalTable
{
Python::Python(
    const StorageID & table_id,
    const StorageInMemoryMetadata & storage_metadata,
    const ASTCreateQuery & create_query_,
    std::unique_ptr<ExternalTableSettings> settings_,
    bool attach_,
    ContextPtr context_)
    : StorageExternalTable(table_id, storage_metadata, std::move(settings_), attach_, context_)
{
    if (!create_query_.exec_script || create_query_.exec_script->empty())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Python external table requires python body defined in CREATE EXTERNAL TABLE ... AS $$...$$");

    python_function = create_query_.exec_script.value();
}

String Python::getType() const
{
    return "Python";
}

void Python::getTableSchema(ContextPtr, ColumnsDescription & desc)
{
    desc = getInMemoryMetadataPtr()->getColumns();
}

void Python::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    init();
    python_storage->read(
        query_plan, column_names, storage_snapshot, query_info, std::move(context_), processed_stage, max_block_size, num_streams);
}

void Python::init()
{
    std::lock_guard lk{mutex};
    if (python_storage)
        return;

    auto function_name = settings->function_name.value.empty() ? getStorageID().getTableName() : settings->function_name.value;
    python_storage
        = StoragePythonTable::create(getStorageID(), getInMemoryMetadataPtr()->getColumns(), std::move(function_name), python_function);
    python_storage->startup();
}
}

void registerPythonExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable(
        "python",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           const ASTCreateQuery & create_query,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) -> StoragePtr {
            return std::make_shared<ExternalTable::Python>(
                table_id, storage_metadata, create_query, std::move(settings), attach, std::move(context));
        });
}
}

#else

namespace DB
{
class ExternalTableFactory;
void registerPythonExternalTable(ExternalTableFactory &)
{
}
}

#endif
