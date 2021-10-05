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
        bool has_virtual_shard_num_column_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        std::vector<QueryPlanPtr> & local_plans,
        Shards & remote_shards,
        UInt32 shard_count) override;

private:
    const Block header;
    QueryProcessingStage::Enum processed_stage;
    /// StorageID main_table = StorageID::createEmpty();
    bool has_virtual_shard_num_column;
};
}
}
