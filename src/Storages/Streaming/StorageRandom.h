#pragma once

#include <optional>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>


namespace DB
{
/* Generates random data for given schema.
 */
class StorageRandom final : public shared_ptr_helper<StorageRandom>, public IStorage
{
    friend struct shared_ptr_helper<StorageRandom>;

public:
    std::string getName() const override { return "Random"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context_,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    UInt64 random_seed = 0;

protected:
    StorageRandom(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::optional<UInt64> random_seed);
};

}
