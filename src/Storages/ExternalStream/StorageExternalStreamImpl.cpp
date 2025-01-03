#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
}

StorageExternalStreamImpl::StorageExternalStreamImpl(IStorage * storage, ExternalStreamSettingsPtr settings_, const ContextPtr & context)
    : IStorage(storage->getStorageID())
    , settings(std::move(settings_))
    , tmpdir(
          fs::path(context->getConfigRef().getString("tmp_path", fs::temp_directory_path())) / "external_streams"
          / toString(getStorageID().uuid))
    , logger(&Poco::Logger::get(getLoggerName()))
{
    /// Make it easier for people to ingest data from external streams. A lot of times people didn't see data coming
    /// only because the external stream does not have all the fields.
    if (!settings->input_format_skip_unknown_fields.changed)
        settings->input_format_skip_unknown_fields = true;

    inferDataFormat(*storage);
    adjustSettingsForDataFormat();
    assert(!data_format.empty());
}

void StorageExternalStreamImpl::createTempDirIfNotExists() const
{
    std::error_code err;
    /// create_directories will do nothing if the directory already exists.
    fs::create_directories(tmpdir, err);
    if (err)
        throw Exception(
            ErrorCodes::CANNOT_CREATE_DIRECTORY,
            "Failed to create external stream temproary directory, error_code={}, error_mesage={}",
            err.value(),
            err.message());
}

void StorageExternalStreamImpl::tryRemoveTempDir() const
{
    LOG_INFO(logger, "Trying to remove external stream temproary directory {}", tmpdir.string());
    std::error_code err;
    fs::remove_all(tmpdir, err);
    if (err)
        LOG_ERROR(logger, "Failed to remove the temporary directory, error_code={}, error_message={}", err.value(), err.message());
}

String StorageExternalStreamImpl::getLoggerName() const
{
    const auto storage_id = getStorageID();
    return storage_id.getDatabaseName() == "default" ? storage_id.getTableName() : storage_id.getFullNameNotQuoted();
}

FormatSettings StorageExternalStreamImpl::getFormatSettings(const ContextPtr & context) const
{
    auto ret = settings->getFormatSettings(context);
    /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
    /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
    ret.protobuf.allow_multiple_rows_without_delimiter = true;
    return ret;
}

void StorageExternalStreamImpl::inferDataFormat(const IStorage & storage)
{
    data_format = settings->data_format.value;
    if (!data_format.empty())
        return;

    /// If there is only one column and its type is a string type, use RawBLOB. Use JSONEachRow otherwise.
    auto column_names_and_types{storage.getInMemoryMetadata().getColumns().getOrdinary()};
    if (column_names_and_types.size() == 1)
    {
        auto type = column_names_and_types.begin()->type->getTypeId();
        if (type == TypeIndex::String || type == TypeIndex::FixedString)
        {
            data_format = "RawBLOB";
            return;
        }
    }

    data_format = "JSONEachRow";
}

void StorageExternalStreamImpl::adjustSettingsForDataFormat()
{
    /// Some formats should always produce one message per row.
    if (data_format == "RawBLOB" || data_format == "ProtobufSingle"
        || (data_format == "Avro" && (!settings->format_schema.value.empty() || !settings->kafka_schema_registry_url.value.empty())))
    {
        if (settings->isChanged("one_message_per_row"))
        {
            /// Can't throw an Exception for now for backward compatibility.
            if (!settings->one_message_per_row.value)
                LOG_INFO(
                    logger,
                    "Setting `one_message_per_row` to `false` with data format {} is discouraged, and could be disabled in the future.",
                    data_format);
        }
        else
        {
            settings->set("one_message_per_row", true);
        }
    }
}

Block StorageExternalStreamImpl::getPhysicalColumns(const StorageSnapshotPtr & storage_snapshot, const Block & header)
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    Block result;
    for (const auto & col : header)
    {
        if (std::any_of(non_virtual_header.begin(), non_virtual_header.end(), [&col](auto & non_virtual_column) {
                return non_virtual_column.name == col.name;
            }))
            result.insert(col);
    }

    /// Clients like to read virtual columns only, add the first physical column, then we know how many rows
    if (result.columns() == 0)
    {
        const auto & physical_columns = storage_snapshot->getColumns(GetColumnsOptions::Ordinary);
        const auto & physical_column = physical_columns.front();
        result.insert({physical_column.type->createColumn(), physical_column.type, physical_column.name});
    }

    return result;
}

std::unique_ptr<StreamingFormatExecutor> StorageExternalStreamImpl::createInputFormatExecutor(
    const StorageSnapshotPtr & storage_snapshot,
    const Block & header,
    ReadBuffer & read_buffer,
    size_t max_block_size,
    const ContextPtr & query_context)
{
    auto physical_header = getPhysicalColumns(storage_snapshot, header);
    /// The buffer is only for initialzing the input format, it won't be actually used, because the executor will keep setting new buffers.
    auto input_format = FormatFactory::instance().getInput(
        data_format,
        read_buffer,
        physical_header,
        query_context,
        max_block_size,
        getFormatSettings(query_context));

    return std::make_unique<StreamingFormatExecutor>(
        physical_header, std::move(input_format), [](const MutableColumns &, Exception & ex) -> size_t { throw std::move(ex); });
}

void StorageExternalStreamImpl::read(
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

    /// Override the maximum concurrency
    auto max_parallel_streams = std::max<size_t>(pipe.maxParallelStreams(), context_->getSettingsRef().min_threads.value);
    query_plan.setMaxThreads(max_parallel_streams);

    auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName(), query_info.storage_limits);
    query_plan.addStep(std::move(read_step));
}

}
