#pragma once

#include "IStreamFactory.h"

#include <Core/QueryProcessingStage.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

using Scalars = std::map<String, Block>;

namespace ClusterProxy
{
class DistributedSelectStreamFactory final : public IStreamFactory
{
public:
    /// Database in a query.
    DistributedSelectStreamFactory(
        const Block & header_,
        QueryProcessingStage::Enum processed_stage_,
        StorageID main_table_,
        const Scalars & scalars_,
        const Tables & external_tables);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        ContextPtr context,
        const ThrottlerPtr & throttler,
        const SelectQueryInfo & query_info,
        std::vector<QueryPlanPtr> & plans,
        Pipes & remote_pipes,
        Pipes & delayed_pipes,
        Poco::Logger * log) override;

private:
    const Block header;
    QueryProcessingStage::Enum processed_stage;
    StorageID main_table = StorageID::createEmpty();
    Scalars scalars;
    Tables external_tables;
};
}
}
