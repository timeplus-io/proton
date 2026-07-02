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

    /// Streams that this external stream pushes ingested data into (used by INPUT dependency tracking).
    /// Push-input external streams override this; the default has no targets.
    virtual std::vector<StorageID> getTargetTables() const { return {}; }

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

    /// Validates external stream for the settings, internal and external dependencies, etc.
    ///
    /// The purpose of this method is that external stream's validity depends on the environment
    /// and may be different on different nodes.
    /// 
    /// Checking these in the constructor will have problems:
    /// - Exception on some of the nodes causes inconsistent result of stream creation
    /// - MetadataUpdater may take long time to connect external resource and even be blocked
    ///
    /// On the contrary, validate() is only called on the stream creation initiator node. It gives the user a instant response
    /// when some settings are incorrect or violate the constraints. After validate() passed, the external stream will
    /// be stored in the cluster metadata.
    /// 
    /// startup() initialize the external stream at runtime such as creating heavy member objects, starting background threads and etc.
    /// It may still fail on external stream loading because of the change of external environmnet.
    /// In this situation, any further operations such as read/write should fail by exception.
    ///
    /// In summary:
    /// - Put the deterministic, fast and no-except operations in ctor.
    /// - Put the dynamic checks in validate() which is only executed on initiator node and may throw exception.
    /// - Put the other initialize operations in startup() which is called on every nodes. Do not throw exception on failure.
    virtual void validate(const ContextPtr & context) const { validateSettings(settings, false, context); }

    /// Check `new_settings` and throw if not meet external stream requirenment.
    /// `change_settings` is set true when verification is during altering current external setting.
    /// The function is expected called when: 1) New external stream is created. 2) ALTER STREAM MODIFY SETTINGS command.
    virtual void validateSettings(const ExternalStreamSettingsPtr & /*new_settings*/, bool /*change_settings*/, const ContextPtr & /*context*/) const = 0;

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
