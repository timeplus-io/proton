#pragma once

#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/IStorage.h>

namespace DB
{
/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl : public std::enable_shared_from_this<StorageExternalStreamImpl>, public IStorage
{
public:
    StorageExternalStreamImpl(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ContextPtr & context);

    virtual ExternalStreamCounterPtr getExternalStreamCounter() const { return nullptr; }

    FormatSettings getFormatSettings(const ContextPtr & context) const;

    /// Some implementations have its own logic to infer the format.
    virtual const String & dataFormat() const { return settings->data_format.value; }

    const String & formatSchema() const { return settings->format_schema.value; }

    bool supportsStreamingQuery() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

protected:
    /// Creates a temporary directory for the external stream to store temporary data.
    void createTempDirIfNotExists() const;
    void tryRemoveTempDir(Poco::Logger * logger) const;

    std::unique_ptr<ExternalStreamSettings> settings;
    fs::path tmpdir;

private:
    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;
};

}
