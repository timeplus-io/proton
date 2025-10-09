#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>

#include <base/shared_ptr_helper.h>


namespace DB
{
class StorageView final : public shared_ptr_helper<StorageView>, public IStorage
{
    friend struct shared_ptr_helper<StorageView>;
public:
    std::string getName() const override { return "View"; }

    bool isView() const override { return true; }
    bool isParameterizedView() const { return is_parameterized_view; }

    bool isLocal() const override { return false; }

    /// It is passed inside the query and solved at its level.
    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool isStreamingQuery(ContextPtr query_context) const;
    bool hasStreamingGlobalAggregation() const;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static void replaceQueryParametersIfParametrizedView(ASTPtr & outer_query, const NameToNameMap & parameter_values);

    static void replaceWithSubquery(ASTSelectQuery & select_query, ASTPtr & view_name, const StorageMetadataPtr & metadata_snapshot, const bool parameterized_view)
    {
        replaceWithSubquery(select_query, metadata_snapshot->getSelectQuery().inner_query->clone(), view_name, parameterized_view);
    }

    static void replaceWithSubquery(ASTSelectQuery & outer_query, ASTPtr view_query, ASTPtr & view_name, const bool parameterized_view);
    static ASTPtr restoreViewName(ASTSelectQuery & select_query, const ASTPtr & view_name);

    /// proton: starts.
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    Streaming::DataStreamSemanticEx dataStreamSemantic() const override;

    NamesAndTypesList getVirtuals() const override;

    void startup() override;
    void shutdown(bool dropping) override;

    bool isReady() const override;

private:
    ContextMutablePtr local_context;
    mutable bool data_stream_semantic_resolved = false;
    std::atomic_flag started;
    /// proton: ends.

protected:
    StorageView(
        const StorageID & table_id_,
        const ASTCreateQuery & query,
        const ColumnsDescription & columns_,
        const String & comment,
        ContextPtr context_,
        const bool is_parameterized_view_);

protected:
    bool is_parameterized_view;
};

}
