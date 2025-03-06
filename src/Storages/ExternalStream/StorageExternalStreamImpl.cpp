#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
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
    /// In case of kafka schema registry is used, the topic name needs to be passed to the output format to fetch the schema.
    ret.kafka_schema_registry.topic_name = settings->topic;
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
