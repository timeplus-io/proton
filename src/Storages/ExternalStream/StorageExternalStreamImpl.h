#pragma once

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/IStorage.h>

namespace DB
{
/// Base class of StorageExternalStreamImpl
class StorageExternalStreamImpl : public std::enable_shared_from_this<StorageExternalStreamImpl>
{
public:
    explicit StorageExternalStreamImpl(std::unique_ptr<ExternalStreamSettings> settings_): settings(std::move(settings_)) {}
    virtual ~StorageExternalStreamImpl() = default;

    virtual void startup() = 0;
    virtual void shutdown() = 0;
    virtual bool supportsSubcolumns() const { return false; }
    virtual NamesAndTypesList getVirtuals() const { return {}; }
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

    /// Get the format settings based on the query settings and external stream settings.
    /// Query settings have higher priority.
    FormatSettings getFormatSettings(const ContextPtr & query_ctx) const
    {
        const auto & query_settings = query_ctx->getSettingsRef();

        auto format_settings = DB::getFormatSettings(query_ctx);
        if (format_settings.schema.format_schema.empty())
            format_settings.schema.format_schema = formatSchema();
        if (!query_settings.isChanged("input_format_skip_unknown_fields"))
            format_settings.skip_unknown_fields = settings->input_format_skip_unknown_fields.value;
        return format_settings;
    }

protected:
    std::unique_ptr<ExternalStreamSettings> settings;
};

}
