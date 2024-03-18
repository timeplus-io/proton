#pragma once

#include <Formats/FormatFactory.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/IStorage.h>

namespace DB
{
/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl : public std::enable_shared_from_this<StorageExternalStreamImpl>
{
public:
    explicit StorageExternalStreamImpl(std::unique_ptr<ExternalStreamSettings> settings_): settings(std::move(settings_)) {
        /// Make it easier for people to ingest data from external streams. A lot of times people didn't see data coming
        /// only because the external stream does not have all the fields.
        if (!settings->input_format_skip_unknown_fields.changed)
            settings->input_format_skip_unknown_fields = true;
    }
    virtual ~StorageExternalStreamImpl() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;
    virtual bool supportsSubcolumns() const { return false; }
    virtual NamesAndTypesList getVirtuals() const { return {}; }
    virtual ExternalStreamCounterPtr getExternalStreamCounter() const { return nullptr; }
    /// Some implementations have its own logic to infer the format.
    virtual const String & dataFormat() const { return settings->data_format.value; }
    const String & formatSchema() const { return settings->format_schema.value; }

    virtual Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams)
        = 0;

    virtual SinkToStoragePtr write(const ASTPtr & /* query */, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* context */)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Ingesting data to this type of external stream is not supported");
    }

    FormatSettings getFormatSettings(const ContextPtr & context) const
    {
        auto ret = settings->getFormatSettings(context);
        /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
        /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
        ret.protobuf.allow_multiple_rows_without_delimiter = true;
        return ret;
    }

protected:
    std::unique_ptr<ExternalStreamSettings> settings;
};

}
