#pragma once

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPHeaderEntries.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>
#include <Storages/ExternalStream/BatchConfiguration.h>


namespace DB::ExternalStream
{

class HTTP final : public StorageExternalStreamImpl
{
public:
    HTTP(
        StorageID storage_id,
        StorageInMemoryMetadata metadata,
        std::unique_ptr<ExternalStreamSettings> settings_,
        ASTStorage * storage_def,
        bool attach,
        ContextPtr context);

    ~HTTP() override = default;

    void validate(const ContextPtr & context) const override;
    void validateSettings(const ExternalStreamSettingsPtr & new_settings, bool change_settings, const ContextPtr & context) const override;

    void startup() override;

    String getName() const override { return "HTTPExternalStream"; }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr &, ContextPtr context) override;

private:
    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    String getReadMethod() const;

    SettingsChanges storage_settings;

    String write_url;
    String write_method;
    HTTPHeaderEntries headers;
    CompressionMethod compression_method;

    String ca_file;
    String key_file;

    /// Stream level settings, can be overrided on queries
    const ConnectionTimeouts timeouts;
    BatchConfiguration batch_config;

    ASTPtr partition_by;

    std::atomic_bool started{false};
};

}
