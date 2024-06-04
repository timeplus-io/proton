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
    explicit StorageExternalStreamImpl(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ContextPtr & context)
        : storage_id(storage->getStorageID())
        , settings(std::move(settings_))
        , tmpdir(fs::path(context->getConfigRef().getString("tmp_path", fs::temp_directory_path())) / "external_streams" / toString(storage_id.uuid))
        {
        /// Make it easier for people to ingest data from external streams. A lot of times people didn't see data coming
        /// only because the external stream does not have all the fields.
        if (!settings->input_format_skip_unknown_fields.changed)
            settings->input_format_skip_unknown_fields = true;
    }

    virtual ExternalStreamCounterPtr getExternalStreamCounter() const { return nullptr; }

    /// Some implementations have its own logic to infer the format.
    virtual const String & dataFormat() const { return settings->data_format.value; }

    const String & formatSchema() const { return settings->format_schema.value; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    FormatSettings getFormatSettings(const ContextPtr & context) const
    {
        auto ret = settings->getFormatSettings(context);
        /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
        /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
        ret.protobuf.allow_multiple_rows_without_delimiter = true;
        return ret;
    }

protected:
    /// Creates a temporary directory for the external stream to store temporary data.
    void createTempDirIfNotExists() const;
    void tryRemoveTempDir(Poco::Logger * logger) const;

    StorageID storage_id;
    std::unique_ptr<ExternalStreamSettings> settings;
    fs::path tmpdir;
};

}
