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

}
