#pragma once

#include <Formats/FormatFactory.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/IStorage.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>

namespace DB
{

/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl : public IStorage
{
public:
    StorageExternalStreamImpl(
        StorageID storage_id,
        StorageInMemoryMetadata storage_metadata,
        ExternalStreamSettingsPtr settings_,
        const ContextPtr & context);

    FormatSettings getFormatSettings(const ContextPtr & context) const;

    const String & dataFormat() const { return data_format; }

    const String & formatSchema() const { return settings->format_schema.value; }

    bool supportsAccurateSeekTo() const noexcept override { return true; }
    bool supportsStreamingQuery() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsParallelInsert() const override { return true; }
    bool squashInsert() const noexcept override { return false; }
    bool prefersLargeBlocks() const override { return false; }

    void startup() override;

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

    bool isLocal() const override { return settings->local.value; }

    /// Check `new_settings` and throw if not meet external stream requirenment.
    /// `change_settings` is set true when verification is during altering current external setting.
    /// The function is expected called when: 1) New external stream is created. 2) ALTER STREAM MODIFY SETTINGS command.
    virtual void verifySettings(const ExternalStreamSettingsPtr & /*new_settings*/, bool /*change_settings*/, ContextPtr /*context_*/) const { }

protected:
    void inferDataFormat();

    /// Creates a temporary directory for the external stream to store temporary data.
    void createTempDirIfNotExists() const;
    void tryRemoveTempDir() const;

protected:
    ExternalStreamSettingsPtr settings;
    fs::path tmpdir;

    String data_format;

    LoggerPtr logger;

private:
    virtual NamesAndTypesList getPhysicalColumns() const;
    void adjustSettingsForDataFormat();

    virtual Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;
};

}
