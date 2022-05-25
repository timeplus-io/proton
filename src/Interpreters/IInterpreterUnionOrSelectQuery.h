#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/IAST_fwd.h>
#include <DataTypes/DataTypesNumber.h>

/// proton: starts.
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
    IInterpreterUnionOrSelectQuery(const ASTPtr & query_ptr_, ContextPtr context_, const SelectQueryOptions & options_)
        : query_ptr(query_ptr_)
        , context(Context::createCopy(context_))
        , options(options_)
        , max_streams(context->getSettingsRef().max_threads)
    {
        if (options.shard_num)
            context->addLocalScalar(
                "_shard_num",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_num), std::make_shared<DataTypeUInt32>(), "_shard_num"}});
        if (options.shard_count)
            context->addLocalScalar(
                "_shard_count",
                Block{{DataTypeUInt32().createColumnConst(1, *options.shard_count), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
    }

    virtual void buildQueryPlan(QueryPlan & query_plan) = 0;
    QueryPipelineBuilder buildQueryPipeline();

    virtual void ignoreWithTotals() = 0;

    virtual ~IInterpreterUnionOrSelectQuery() override = default;

    Block getSampleBlock() { return result_header; }

    size_t getMaxStreams() const { return max_streams; }

    /// proton: starts
    virtual bool hasAggregation() const = 0;
    virtual bool isStreaming() const = 0;
    virtual bool hasGlobalAggregation() const = 0;
    virtual bool hasStreamingWindowFunc() const = 0;

    /// Return the object column descriptions of the current query to provide subcolumns information to downstream 
    /// pipeline. If the current query doesn't have any object columns, return empty but non-nullptr ColumnsDescription.
    virtual ColumnsDescriptionPtr getExtendedObjects() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "not implemented"); }
    /// proton: ends

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr &, ContextPtr) const override;

protected:
    ASTPtr query_ptr;
    ContextMutablePtr context;
    Block result_header;
    SelectQueryOptions options;
    size_t max_streams = 1;
    bool settings_limit_offset_needed = false;
    bool settings_limit_offset_done = false;
};
}

