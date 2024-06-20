#pragma once

#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/StorageProxy.h>

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
    ExternalStreamCounterPtr getExternalStreamCounter() { return external_stream_counter; }

    StoragePtr getNested() const override { return external_stream; }

    String getName() const override { return "ExternalStream"; }

    bool supportsSubcolumns() const override { return external_stream->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return true; /*return external_stream->supportsStreamingQuery();*/ }
    bool supportsAccurateSeekTo() const noexcept override { return external_stream->supportsAccurateSeekTo(); }
    bool squashInsert() const noexcept override { return external_stream->squashInsert(); }

    friend class KafkaSource;

    void read(
            QueryPlan & query_plan,
            const Names & column_names,
            const StorageSnapshotPtr & storage_snapshot,
            SelectQueryInfo & query_info,
            ContextPtr context_,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            size_t num_streams) override
    {
        String cnames;
        for (const auto & c : column_names)
            cnames += c + " ";
        auto storage = getNested();
        auto nested_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context_);
        storage->read(query_plan, column_names, nested_snapshot, query_info, context_,
                                  processed_stage, max_block_size, num_streams);
        bool add_conversion{true};
        if (add_conversion)
        {
            auto from_header = query_plan.getCurrentDataStream().header;
            auto to_header = getHeaderForProcessingStage(column_names, storage_snapshot,
                                                         query_info, context_, processed_stage);

            auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                    from_header.getColumnsWithTypeAndName(),
                    to_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name);

            auto step = std::make_unique<ExpressionStep>(
                query_plan.getCurrentDataStream(),
                convert_actions_dag);

            step->setStepDescription("Converting columns");
            query_plan.addStep(std::move(step));
        }
    }

    SinkToStoragePtr write(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            ContextPtr context_) override
    {
        auto storage = getNested();
        auto cached_structure = metadata_snapshot->getSampleBlock();
        auto actual_structure = storage->getInMemoryMetadataPtr()->getSampleBlock();
        auto add_conversion{true};
        if (!blocksHaveEqualStructure(actual_structure, cached_structure) && add_conversion)
        {
            throw Exception("Source storage and table function have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
        }
        return storage->write(query, metadata_snapshot, context_);
    }
protected:
    StorageExternalStream(
        const ASTs & engine_args,
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<ExternalStreamSettings> external_stream_settings_,
        const String & comment,
        bool attach);

private:
    ExternalStreamCounterPtr external_stream_counter;
    StoragePtr external_stream;
};

}
