#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>
#include <DataTypes/DataTypesNumber.h>

/// proton: starts.
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Streaming/JoinStreamDescription.h>
#include <Storages/ColumnsDescription.h>
/// proton: ends.

namespace DB
{

/// proton: starts.
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
/// proton: ends.

class IInterpreterUnionOrSelectQuery : public IInterpreter
{
public:
    IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, const ContextPtr & context_, const SelectQueryOptions & options_)
        : IInterpreterUnionOrSelectQuery(query_ptr_, Context::createCopy(context_), options_)
    {
    }

    IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_, const SelectQueryOptions & options_)
        : query_ptr(query_ptr_)
        , context(context_)
        , options(options_)
        , max_streams(context->getSettingsRef().max_threads)
    {
        if (options.shard_num)
        {
            context->addSpecialScalar(
                "_shard_num",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}});

            /// proton: starts.
            /// This is a distirbuted query on local node, get required shard_num
            context->setShardToRead(*options.shard_num - 1);  /// based on 0
            /// proton: ends.
        }

        if (options.shard_count)
            context->addSpecialScalar(
                "_shard_count",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});

        /// proton: starts.
        /// This is a distirbuted query on remote node, get required shard_num
        if (context->hasQueryContext() && context->getQueryContext()->hasScalar("_shard_num"))
        {
            const auto * column = checkAndGetColumn<ColumnVector<UInt32>>(
                context->getQueryContext()->getScalar("_shard_num").getByPosition(0).column.get());
            if (unlikely(!column))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't get shard num from distributed query on remote node");

            context->setShardToRead(column->getData()[0] - 1);  /// based on 0
        }
        /// proton: ends.
    }

    virtual void buildQueryPlan(QueryPlan & query_plan) = 0;
    QueryPipelineBuilder buildQueryPipeline();

    virtual void ignoreWithTotals() = 0;

    ~IInterpreterUnionOrSelectQuery() override = default;

    Block getSampleBlock() { return result_header; }

    size_t getMaxStreams() const { return max_streams; }

    /// proton: starts
    virtual bool hasAggregation() const = 0;
    virtual bool isStreaming() const = 0;
    virtual bool hasGlobalAggregation() const = 0;
    virtual bool hasStreamingWindowFunc() const = 0;
    virtual Streaming::DataStreamSemantic getDataStreamSemantic() const = 0;

    /// Return the object column descriptions of the current query to provide subcolumns information to downstream
    /// pipeline. If the current query doesn't have any object columns, return empty but non-nullptr ColumnsDescription.
    virtual ColumnsDescriptionPtr getExtendedObjects() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not implemented"); }

    virtual std::set<String> getGroupByColumns() const =0;
    /// proton: ends

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override;

    /// Add limits from external query.
    void addStorageLimits(const StorageLimitsList & limits);

protected:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    Block result_header;
    SelectQueryOptions options;
    StorageLimitsList storage_limits;

    size_t max_streams = 1;
    bool settings_limit_offset_needed = false;
    bool settings_limit_offset_done = false;

    /// Set quotas to query pipeline.
    void setQuota(QueryPipeline & pipeline) const;

    static StorageLimits getStorageLimits(const Context & context, const SelectQueryOptions & options);
};
}
