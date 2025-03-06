#pragma once

#include <Common/logger_useful.h>
#include <Formats/FormatFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
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
    StorageExternalStreamImpl(IStorage * storage, ExternalStreamSettingsPtr settings_, const ContextPtr & context);

    FormatSettings getFormatSettings(const ContextPtr & context) const;

    const String & dataFormat() const { return data_format; }

    const String & formatSchema() const { return settings->format_schema.value; }

    bool supportsAccurateSeekTo() const noexcept override { return true; }
    bool supportsStreamingQuery() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool squashInsert() const noexcept override { return false; }

    virtual std::vector<int64_t> getLastSNs() const { return {}; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getLoggerName() const;

protected:
    void inferDataFormat(const IStorage & storage);

    /// Creates a temporary directory for the external stream to store temporary data.
    void createTempDirIfNotExists() const;
    void tryRemoveTempDir() const;

    ExternalStreamSettingsPtr settings;
    fs::path tmpdir;

    String data_format;

    Poco::Logger * logger;

private:
    void adjustSettingsForDataFormat();

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override = 0;
};

}
