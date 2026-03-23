#include <Storages/ExternalStream/StorageExternalStream.h>

#include <IO/Kafka/Connection.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Formats/ISchemaReader.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/HTTP/HTTP.h>
#include <Storages/ExternalStream/Iceberg/Iceberg.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Log/FileLog.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#if USE_NATSIO
#include <Storages/ExternalStream/NATSJetstream/NATSJetstream.h>
#endif
#if USE_PYTHON_UDF
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#endif
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/Timeplus/Timeplus.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>

#include <boost/algorithm/string.hpp>
#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/KeyFileHandler.h>
#include <Poco/Net/SSLManager.h>

#include <string>
#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int INCORRECT_QUERY;
extern const int INVALID_SETTING_VALUE;
extern const int NOT_IMPLEMENTED;
extern const int TYPE_MISMATCH;
}

namespace
{
ExpressionActionsPtr buildShardingKeyExpression(ASTPtr sharding_key, ContextPtr context, const NamesAndTypesList & columns)
{
    auto syntax_result = TreeRewriter(context).analyze(sharding_key, columns);
    return ExpressionAnalyzer(sharding_key, syntax_result, context).getActions(true);
}

void validateEngineArgs(ContextPtr context, ASTs & engine_args, const ColumnsDescription & columns)
{
    if (engine_args.empty())
        return;

    auto sharding_expr = buildShardingKeyExpression(engine_args[0], context, columns.getAllPhysical());
    const auto & block = sharding_expr->getSampleBlock();
    if (block.columns() != 1)
        throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Sharding expression must return exactly one column");

    auto type = block.getByPosition(0).type;

    if (!type->isValueRepresentedByInteger())
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Sharding expression has type {}, but should be one of integer type", type->getName());
}

StoragePtr createExternalStream(
    StorageID storage_id,
    StorageInMemoryMetadata storage_metadata,
    std::unique_ptr<ExternalStreamSettings> external_stream_settings,
    const ASTs & engine_args,
    ASTStorage * storage_def,
    bool attach,
    [[maybe_unused]] const std::optional<String> & exec_script,
    ExternalStreamCounterPtr external_stream_counter,
    ContextPtr context)
{
    auto type = external_stream_settings->type.value;

    if (type.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream type is required in settings");

    if (type == StreamTypes::KAFKA || type == StreamTypes::REDPANDA)
        return std::make_unique<ExternalStream::Kafka>(
            std::move(storage_id),
            std::move(storage_metadata),
            std::move(external_stream_settings),
            engine_args,
            attach,
            std::move(external_stream_counter),
            std::move(context));

    if (type == StreamTypes::TIMEPLUS)
        return std::make_unique<ExternalStream::Timeplus>(
            std::move(storage_id), std::move(storage_metadata), std::move(external_stream_settings), attach, std::move(context));

    if (type == StreamTypes::LOG && context->getSettingsRef()._tp_enable_log_stream_expr.value)
        return std::make_unique<FileLog>(
            std::move(storage_id), std::move(storage_metadata), std::move(external_stream_settings), std::move(context));

    if (type == StreamTypes::HTTP)
        return std::make_unique<ExternalStream::HTTP>(
            std::move(storage_id),
            std::move(storage_metadata),
            std::move(external_stream_settings),
            storage_def,
            attach,
            std::move(context));

#if USE_PULSAR
    if (type == StreamTypes::PULSAR)
        return std::make_unique<ExternalStream::Pulsar>(
            std::move(storage_id),
            std::move(storage_metadata),
            std::move(external_stream_settings),
            attach,
            std::move(external_stream_counter),
            std::move(context));

#endif

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET
    if (type == StreamTypes::ICEBERG)
        return std::make_unique<ExternalStream::Iceberg>(
            std::move(storage_id),
            std::move(storage_metadata),
            std::move(external_stream_settings),
            std::move(external_stream_counter),
            std::move(context));

#endif

#if USE_NATSIO
    if (type == StreamTypes::NATS_JETSTREAM)
        return std::make_unique<ExternalStream::NATSJetstream>(
            std::move(storage_id),
            std::move(storage_metadata),
            std::move(external_stream_settings),
            attach,
            std::move(external_stream_counter),
            std::move(context));
#endif
#if USE_PYTHON_UDF
    if (type == ExternalStreamTypes::PYTHON)
    {
        if (!exec_script || exec_script->empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Python external stream requires python body defined in CREATE EXTERNAL STREAM ... AS $$...$$");

        String function_name = external_stream_settings->read_function_name.value;
        if (function_name.empty())
            function_name = storage_id.getTableName();

        PythonTableMode mode = PythonTableMode::Auto;
        String mode_value = external_stream_settings->mode.value;
        boost::algorithm::trim(mode_value);
        boost::algorithm::to_lower(mode_value);
        if (mode_value.empty() || mode_value == "auto")
        {
            mode = PythonTableMode::Auto;
        }
        else if (mode_value == "streaming")
        {
            mode = PythonTableMode::Streaming;
        }
        else if (mode_value == "batch")
        {
            mode = PythonTableMode::Batch;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode '{}', expected 'auto', 'streaming', or 'batch'", mode_value);
        }

        String sink_function_name = external_stream_settings->write_function_name.value;
        return StoragePythonTable::create(
            storage_id, storage_metadata.getColumns(), std::move(function_name), *exec_script, mode, std::move(sink_function_name));
    }
#else
    if (type == ExternalStreamTypes::PYTHON)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python external stream is disabled, rebuild with USE_PYTHON_UDF");
#endif
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown external stream type: {}", type);
}
}

Int64 StorageExternalStream::getMetricTime(bool reset)
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->getMetricTime(reset);
}

UInt64 StorageExternalStream::readBytes(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->readBytes(reset);
}

UInt64 StorageExternalStream::readRows(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->readRows(reset);
}

UInt64 StorageExternalStream::writtenBytes(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->writtenBytes(reset);
}

UInt64 StorageExternalStream::writtenRows(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->writtenRows(reset);
}

UInt64 StorageExternalStream::readFailed(bool reset) const
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->readFailed(reset);
}

UInt64 StorageExternalStream::writtenFailed(bool reset) const
{
    if (!external_stream_counter)
        return 0;
    return external_stream_counter->writtenFailed(reset);
}

namespace
{
ColumnsDescription getFormatColumns(const String & format_name, ContextPtr & context, const std::optional<FormatSettings> & format_settings)
{
    if (!FormatFactory::instance().checkIfFormatHasExternalSchemaReader(format_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Format does not have schema reader: {}", format_name);

    const auto external_schema_reader = FormatFactory::instance().getExternalSchemaReader(format_name, context, format_settings);
    auto names_and_types = external_schema_reader->readSchema();
    return ColumnsDescription(std::move(names_and_types));
}
}

ColumnsDescription StorageExternalStream::initColumnsDescription(
    const ColumnsDescription & columns, const ExternalStreamSettings & external_stream_settings, ContextPtr & context_)
{
    if (!columns.empty())
        return columns;

    const auto & stream_type = external_stream_settings.type.value;
    /// Return empty columns for streams allowing empty columns list
    if (stream_type == StreamTypes::TIMEPLUS)
        return columns;

    /// Try to infer columns from the format external schema
    if (stream_type == StreamTypes::KAFKA || stream_type == StreamTypes::REDPANDA || stream_type == StreamTypes::PULSAR
        || stream_type == StreamTypes::NATS_JETSTREAM)
    {
        if (external_stream_settings.data_format.value.empty())
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Incorrect CREATE query: required list of column descriptions or data format setting to infer schema");

        auto format_settings = external_stream_settings.getFormatSettings(context_);
        auto inferred_columns = getFormatColumns(external_stream_settings.data_format, context_, format_settings);
        if (inferred_columns.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Can not infer column descriptions from format settings: stream_type={} data_format={}",
                stream_type,
                external_stream_settings.data_format.value);

        return inferred_columns;
    }

    throw Exception(
        ErrorCodes::INCORRECT_QUERY, "Incorrect CREATE query: required list of column descriptions: stream_type={}", stream_type);
}

StorageExternalStream::StorageExternalStream(
    const ASTs & engine_args,
    const StorageID & table_id_,
    ContextPtr context_,
    const ColumnsDescription & columns_,
    Int32 schema_version,
    const String & comment,
    ASTStorage * storage_def,
    bool attach,
    const std::optional<String> & exec_script)
    : StorageProxy(table_id_), WithContext(context_->getGlobalContext()), external_stream_counter(std::make_shared<ExternalStreamCounter>())
{
    auto external_stream_settings = std::make_unique<ExternalStreamSettings>();
    external_stream_settings->loadFromQuery(*storage_def);

    if (!external_stream_settings->config_file.value.empty())
        external_stream_settings->loadFromConfigFile(external_stream_settings->config_file.value);

    if (!external_stream_settings->named_collection.value.empty())
        updateSettingsByNamedCollection(*external_stream_settings, context_);

    external_stream_type = external_stream_settings->type.value;
    ColumnsDescription columns = initColumnsDescription(columns_, *external_stream_settings, context_);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setVersion(schema_version);
    storage_metadata.setComment(comment);
    if (storage_def->partition_by != nullptr)
    {
        ASTPtr partition_by_ast{storage_def->partition_by->clone()};
        storage_metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_ast, columns, context_);
    }

    if (!columns.empty())
        storage_metadata.setColumns(std::move(columns));

    auto stream = createExternalStream(
        this->getStorageID(),
        std::move(storage_metadata),
        std::move(external_stream_settings),
        engine_args,
        storage_def,
        attach,
        exec_script,
        external_stream_counter,
        std::move(context_));
    external_stream.swap(stream);
    /// Some external streams fetch the structure in other ways, thus need to set the metadata again here in case it's updated.
    setInMemoryMetadata(external_stream->getInMemoryMetadata());
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
    getNested()->read(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

void StorageExternalStream::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter command is not supported for external stream yet");
}

void registerStorageExternalStream(StorageFactory & factory)
{
    /** * ExternalStream engine arguments : ExternalStream(shard_by_expr)
    * - shard_by_expr
    **/
    auto creator_fn = [](const StorageFactory::Arguments & args) {
        validateEngineArgs(args.getLocalContext(), args.engine_args, args.columns);

        if (args.storage_def->settings != nullptr)
            return StorageExternalStream::create(
                args.engine_args,
                args.table_id,
                args.getContext(),
                args.columns,
                args.schema_version,
                args.comment,
                args.storage_def,
                args.attach,
                args.query.exec_script);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "External stream requires correct settings setup");
    };

    factory.registerStorage(
        "ExternalStream",
        creator_fn,
        StorageFactory::StorageFeatures{
            .supports_settings = true,
            .supports_sort_order = true, // for partition by
            .supports_schema_inference = true,
        });
}
}
