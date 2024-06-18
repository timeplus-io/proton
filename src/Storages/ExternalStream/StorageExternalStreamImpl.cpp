#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include "Processors/QueryPlan/QueryPlan.h"
#include "Processors/QueryPlan/ReadFromPreparedSource.h"
#include "Storages/SelectQueryInfo.h"

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
}

namespace
{
StorageID getID(IStorage * storage) {
    auto * logger = &Poco::Logger::get("getStorageID");
    LOG_INFO(logger, "Called!");
    return storage->getStorageID();
}
}

StorageExternalStreamImpl::StorageExternalStreamImpl(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ContextPtr & context)
    : IStorage(getID(storage))
    , settings(std::move(settings_))
    , tmpdir(fs::path(context->getConfigRef().getString("tmp_path", fs::temp_directory_path())) / "external_streams" / toString(getStorageID().uuid))
    {
    auto * logger = &Poco::Logger::get("ExternalStreamImpl");
    LOG_INFO(logger, "Creating!!!");
    /// Make it easier for people to ingest data from external streams. A lot of times people didn't see data coming
    /// only because the external stream does not have all the fields.
    if (!settings->input_format_skip_unknown_fields.changed)
        settings->input_format_skip_unknown_fields = true;
}

void StorageExternalStreamImpl::createTempDirIfNotExists() const
{
    std::error_code err;
    /// create_directories will do nothing if the directory already exists.
    fs::create_directories(tmpdir, err);
    if (err)
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "Failed to create external stream temproary directory, error_code={}, error_mesage={}", err.value(), err.message());
}

void StorageExternalStreamImpl::tryRemoveTempDir(Poco::Logger * logger) const
{
    LOG_INFO(logger, "Trying to remove external stream temproary directory {}", tmpdir.string());
    std::error_code err;
    fs::remove_all(tmpdir, err);
    if (err)
        LOG_ERROR(logger, "Failed to remove the temporary directory, error_code={}, error_message={}", err.value(), err.message());
}

FormatSettings StorageExternalStreamImpl::getFormatSettings(const ContextPtr & context) const
{
    auto ret = settings->getFormatSettings(context);
    /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
    /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
    ret.protobuf.allow_multiple_rows_without_delimiter = true;
    return ret;
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

    auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), getName(), query_info.storage_limits);

    /// Override the maximum concurrency
    auto min_threads = context_->getSettingsRef().min_threads.value;
    if (min_threads > 0)
        query_plan.setMaxThreads(min_threads);
    query_plan.addStep(std::move(read_step));
}

Pipe StorageExternalStreamImpl::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception("Method read is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

}
