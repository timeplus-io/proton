#pragma once

#include <Parsers/ASTCreateQuery.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/IStorage.h>
#include <Storages/StorageProxy.h>
#include <Common/SettingsChanges.h>

#include <base/shared_ptr_helper.h>

namespace DB
{
struct ExternalStreamSettings;

namespace ErrorCodes
{
extern const int INCOMPATIBLE_COLUMNS;
}

/// StorageExternalStream acts like a routing storage engine which proxy the requests to the underlying specific
/// external streaming storage like Kafka, Redpanda etc.
class StorageExternalStream final : public shared_ptr_helper<StorageExternalStream>, public StorageProxy, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalStream>;

public:
    ExternalStreamCounterPtr getExternalStreamCounter() const { return external_stream_counter; }

    StoragePtr getNested() const override { return external_stream; }

    bool isLocal() const override { return external_stream->isLocal(); }

    bool isRemote() const override { return external_stream->isRemote(); }

    String getName() const override { return "ExternalStream"; }

    void startup() override { external_stream->startup(); }

    bool supportsSubcolumns() const override { return external_stream->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return external_stream->supportsStreamingQuery(); }
    bool supportsAccurateSeekTo() const noexcept override { return external_stream->supportsAccurateSeekTo(); }
    bool supportsParallelInsert() const override { return external_stream->supportsParallelInsert(); }
    bool squashInsert() const noexcept override { return external_stream->squashInsert(); }
    bool prefersLargeBlocks() const override { return external_stream->prefersLargeBlocks(); }

    Int64 getMetricTime(bool reset = true) override;
    UInt64 readBytes(bool reset = true, bool external_ingress = true) override;
    UInt64 readRows(bool reset = true, bool external_ingress = true) override;
    UInt64 writtenBytes(bool reset = true, bool external_ingress = true) override;
    UInt64 writtenRows(bool reset = true, bool external_ingress = true) override;

    UInt64 readFailed(bool reset = true) const;
    UInt64 writtenFailed(bool reset = true) const;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getType() const { return external_stream_type; }

    std::optional<String> preferredColumn() const override { return external_stream->preferredColumn(); }

protected:
    StorageExternalStream(
        const ASTs & engine_args,
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        const String & comment,
        ASTStorage * storage_def,
        bool attach);

private:
    static ColumnsDescription initColumnsDescription(
        const ColumnsDescription & columns, const ExternalStreamSettings & external_stream_settings, ContextPtr & context_);

    ExternalStreamCounterPtr external_stream_counter;
    StoragePtr external_stream;
    String external_stream_type;
};

}
