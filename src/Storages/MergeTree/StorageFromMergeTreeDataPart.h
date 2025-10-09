#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <DataTypes/ObjectUtils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Core/Defines.h>
#include <Common/Exception.h>

#include <base/shared_ptr_helper.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart final : public shared_ptr_helper<StorageFromMergeTreeDataPart>, public IStorage
{
public:
    /// Used in part mutation.
    StorageFromMergeTreeDataPart(const MergeTreeData::DataPartPtr & part_)
        : IStorage(getIDFromPart(part_))
        , parts({part_})
        , alter_conversions({part_->storage.getAlterConversionsForPart(part_)})
        , storage(part_->storage)
        , partition_id(part_->info.partition_id)
    {
        setInMemoryMetadata(storage.getInMemoryMetadata());
    }

    /// Used in queries with projection.
    StorageFromMergeTreeDataPart(const MergeTreeData & storage_, MergeTreeDataSelectAnalysisResultPtr analysis_result_ptr_)
        : IStorage(storage_.getStorageID()), storage(storage_), analysis_result_ptr(analysis_result_ptr_)
    {
        setInMemoryMetadata(storage.getInMemoryMetadata());
    }

    String getName() const override { return "FromMergeTreeDataPart"; }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr) const override
    {
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams) override
    {
        query_plan = std::move(*MergeTreeDataSelectExecutor(storage)
                                              .readFromParts(
                                                  parts,
                                                  alter_conversions,
                                                  column_names,
                                                  storage_snapshot,
                                                  query_info,
                                                  context,
                                                  max_block_size,
                                                  num_streams,
                                                  nullptr,
                                                  analysis_result_ptr));
    }

    bool supportsPrewhere() const override { return true; }

    bool supportsDynamicSubcolumnsDeprecated() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }

    bool mayBenefitFromIndexForIn(
        const ASTPtr & left_in_operand, ContextPtr query_context, const StorageMetadataPtr & metadata_snapshot) const override
    {
        return storage.mayBenefitFromIndexForIn(left_in_operand, query_context, metadata_snapshot);
    }

    NamesAndTypesList getVirtuals() const override
    {
        return storage.getVirtuals();
    }

    String getPartitionId() const
    {
        return partition_id;
    }

    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context) const
    {
        return storage.getPartitionIDFromQuery(ast, context);
    }

    bool materializeTTLRecalculateOnly() const
    {
        if (parts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "parts must not be empty for materializeTTLRecalculateOnly");
        return parts.front()->storage.getSettings()->materialize_ttl_recalculate_only;
    }

    bool hasLightweightDeletedMask() const override
    {
        return !parts.empty() && parts.front()->hasLightweightDelete();
    }

    bool supportsLightweightDelete() const override
    {
        return !parts.empty() && parts.front()->supportLightweightDeleteMutate();
    }

private:
    const MergeTreeData::DataPartsVector parts;
    const std::vector<AlterConversionsPtr> alter_conversions;
    const MergeTreeData & storage;
    const String partition_id;
    const MergeTreeDataSelectAnalysisResultPtr analysis_result_ptr;

    static StorageID getIDFromPart(const MergeTreeData::DataPartPtr & part_)
    {
        auto table_id = part_->storage.getStorageID();
        return StorageID(table_id.database_name, table_id.table_name + " (part " + part_->name + ")");
    }
};

}
