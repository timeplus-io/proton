#include <Storages/ExternalStream/Python/StoragePythonTableWithInput.h>

#if USE_PYTHON_UDF

#include <CPython/Utils.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSubquery.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Transforms/PythonTableTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <Storages/SeekToInfo.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/Stream/storageUtil.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
extern const int BAD_ARGUMENTS;
}

StoragePythonTableWithInput::StoragePythonTableWithInput(
    const StorageID & table_id,
    const ColumnsDescription & output_columns,
    cpython::PythonFunction function_,
    ASTPtr source_ast_,
    StoragePtr source_storage_,
    Names input_column_names_,
    PythonTableMode mode_,
    ContextPtr)
    : IStorage(table_id)
    , function(std::move(function_))
    , source_ast(std::move(source_ast_))
    , source_storage(std::move(source_storage_))
    , input_column_names(std::move(input_column_names_))
    , mode(mode_)
{
    StorageInMemoryMetadata metadata;
    metadata.setColumns(output_columns);
    setInMemoryMetadata(metadata);
}

StoragePtr StoragePythonTableWithInput::create(
    const StorageID & table_id,
    const ColumnsDescription & output_columns,
    cpython::PythonFunction function_,
    ASTPtr source_ast_,
    StoragePtr source_storage_,
    Names input_column_names_,
    PythonTableMode mode_,
    ContextPtr context_)
{
    return std::shared_ptr<StoragePythonTableWithInput>(new StoragePythonTableWithInput(
        table_id,
        output_columns,
        std::move(function_),
        std::move(source_ast_),
        std::move(source_storage_),
        std::move(input_column_names_),
        mode_,
        std::move(context_)));
}

void StoragePythonTableWithInput::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    if (function.source_code.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python external stream requires non-empty source");

    QueryPlanResourceHolder resources;
    QueryPipelineBuilderPtr pipeline_builder;

    if (source_ast)
    {
        /// Source is a subquery
        auto * subquery = source_ast->as<ASTSubquery>();
        if (!subquery)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Source for Python external stream must be a subquery");

        InterpreterSelectWithUnionQuery interpreter(subquery->children[0], context, SelectQueryOptions().subquery(), input_column_names);
        pipeline_builder = std::make_unique<QueryPipelineBuilder>(interpreter.buildQueryPipeline());
    }
    else if (source_storage)
    {
        /// Source is a table/stream
        auto source_metadata = source_storage->getInMemoryMetadataPtr();
        auto source_snapshot = source_storage->getStorageSnapshot(source_metadata, context);

        SelectQueryInfo source_query_info;
        source_query_info.query = query_info.query;
        source_query_info.seek_to_info = query_info.seek_to_info ? query_info.seek_to_info : std::make_shared<SeekToInfo>("");
        source_query_info.seek_to_info_of_right_stream = query_info.seek_to_info_of_right_stream;
        auto fallback_result
            = std::make_shared<TreeRewriterResult>(source_metadata->getColumns().getAll(), source_storage, source_snapshot);
        fallback_result->streaming = isStreamingStorage(source_storage, context);
        fallback_result->analyzed_join = std::make_shared<TableJoin>();
        source_query_info.syntax_analyzer_result = std::move(fallback_result);

        QueryPlan source_plan;
        source_storage->read(
            source_plan,
            input_column_names,
            source_snapshot,
            source_query_info,
            context,
            QueryProcessingStage::FetchColumns,
            max_block_size,
            1);

        if (source_plan.isInitialized())
        {
            pipeline_builder = source_plan.buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context), context);
        }
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Source for Python external stream is not set");
    }

    if (!pipeline_builder || !pipeline_builder->initialized())
    {
        IStorage::readFromPipe(query_plan, Pipe{}, column_names, storage_snapshot, query_info, context, getName());
        return;
    }

    Block output_header = storage_snapshot->getSampleBlockForColumns(column_names);
    ColumnsDescription output_columns_local = getInMemoryMetadataPtr()->getColumns();
    const auto function_local = function;
    Names input_names = input_column_names;
    Names output_names = column_names;
    PythonTableMode mode_value = mode;

    auto session = cpython::PythonModuleSession::create("PythonTableWithInput", function_local);

    pipeline_builder->addSimpleTransform(
        [session, input_names, output_columns_local, output_header, output_names, mode_value](const Block & header) {
            return std::make_shared<PythonTableTransform>(
                header, output_header, input_names, output_columns_local, output_names, mode_value, session);
        });

    auto pipe = QueryPipelineBuilder::getPipe(std::move(*pipeline_builder), resources);
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, context, getName());
    query_plan.addResources(std::move(resources));
}
}

#endif
