#include <Storages/ExternalStream/StorageExternalStream.h>

#ifdef OS_LINUX
#    include <Storages/ExternalStream/Log/FileLog.h>
#endif

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/StorageFactory.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Proton/Proton.h>

#include <re2/re2.h>
#include <string>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int NOT_IMPLEMENTED;
extern const int TYPE_MISMATCH;
}

namespace
{
ExpressionActionsPtr
buildShardingKeyExpression(ASTPtr sharding_key, ContextPtr context, const NamesAndTypesList & columns)
{
    auto syntax_result = TreeRewriter(context).analyze(sharding_key, columns);
    return ExpressionAnalyzer(sharding_key, syntax_result, context).getActions(true);
}

void validateEngineArgs(ContextPtr context, ASTs & engine_args, const ColumnsDescription & columns) {
    if (engine_args.empty())
        return;

    auto sharding_expr = buildShardingKeyExpression(engine_args[0], context, columns.getAllPhysical());
    const auto & block = sharding_expr->getSampleBlock();
    if (block.columns() != 1)
        throw Exception("Sharding expression must return exactly one column", ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS);

    auto type = block.getByPosition(0).type;

    if (!type->isValueRepresentedByInteger())
        throw Exception(
            ErrorCodes::TYPE_MISMATCH, "Sharding expression has type {}, but should be one of integer type", type->getName());
}

std::unique_ptr<StorageExternalStreamImpl> createExternalStream(
    IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings, ContextPtr context [[maybe_unused]], const ASTs & engine_args, bool attach, ExternalStreamCounterPtr external_stream_counter, ContextPtr context_)
{
    if (settings->type.value.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream type is required in settings");

    if (settings->type.value == StreamTypes::KAFKA || settings->type.value == StreamTypes::REDPANDA)
        return std::make_unique<Kafka>(storage, std::move(settings), engine_args, attach, external_stream_counter, std::move(context_));

    if (settings->type.value == StreamTypes::PROTON)
        return std::make_unique<ExternalStream::Proton>(storage, std::move(settings), std::move(context_));

#ifdef OS_LINUX
    else if (settings->type.value == StreamTypes::LOG && context->getSettingsRef()._tp_enable_log_stream_expr.value)
        return std::make_unique<FileLog>(storage, std::move(settings), std::move(context_));
    else
#endif
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} external stream is not supported yet", settings->type.value);
}
}

void StorageExternalStream::startup()
{
    external_stream->startup();
}

void StorageExternalStream::shutdown()
{
    external_stream->shutdown();
}

bool StorageExternalStream::supportsSubcolumns() const
{
    return external_stream->supportsSubcolumns();
}

NamesAndTypesList StorageExternalStream::getVirtuals() const
{
    return external_stream->getVirtuals();
}

std::optional<UInt64> StorageExternalStream::totalRows(const Settings & settings) const
{
    return external_stream->totalRows(settings);
}

Pipe StorageExternalStream::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    return external_stream->read(column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

void StorageExternalStream::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    Pipe pipe = read(column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);

    auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName(), query_info.storage_limits);

    /// Override the maximum concurrency
    auto min_threads = context_->getSettingsRef().min_threads.value;
    if (min_threads > 0)
        query_plan.setMaxThreads(min_threads);
    query_plan.addStep(std::move(read_step));
}

ExternalStreamCounterPtr StorageExternalStream::getExternalStreamCounter()
{
    return external_stream->getExternalStreamCounter();
}

SinkToStoragePtr StorageExternalStream::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
{
    return external_stream->write(query, metadata_snapshot, context_);
}

StorageExternalStream::StorageExternalStream(
    const ASTs & engine_args,
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    std::unique_ptr<ExternalStreamSettings> external_stream_settings_,
    const String & comment,
    bool attach)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    auto stream = createExternalStream(this, std::move(external_stream_settings_), context_, engine_args, attach, std::make_shared<ExternalStreamCounter>(), std::move(context_));
    external_stream.swap(stream);
}

void registerStorageExternalStream(StorageFactory & factory)
{
    /** * ExternalStream engine arguments : ExternalStream(shard_by_expr)
    * - shard_by_expr
    **/
    auto creator_fn = [](const StorageFactory::Arguments & args) {
        validateEngineArgs(args.getLocalContext(), args.engine_args, args.columns);

        if (args.storage_def->settings)
        {
            auto external_stream_settings = std::make_unique<ExternalStreamSettings>();
            external_stream_settings->loadFromQuery(*args.storage_def);

            return StorageExternalStream::create(
                args.engine_args, args.table_id, args.getContext(), args.columns, std::move(external_stream_settings), args.comment, args.attach);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream requires correct settings setup");
    };

    factory.registerStorage(
        "ExternalStream",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
        });
}

}
