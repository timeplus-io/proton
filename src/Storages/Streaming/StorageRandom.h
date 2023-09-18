#pragma once

#include <optional>
#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Storages/IStorage.h>
#include <base/shared_ptr_helper.h>


namespace DB
{

class ASTStorage;

#define STORAGE_RANDOM_RELATED_SETTINGS(M) \
    M(UInt64, eps, 1000, "Limit how many rows to be generated per second for each thread. Used by RANDOM STREAM. 0 means no limit", 0) \
    M(UInt64, interval_time, 100, "the data generating interval, unit ms", 0)

#define LIST_OF_STORAGE_RANDOM_SETTINGS(M) \
    STORAGE_RANDOM_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(StorageRandomSettingsTraits, LIST_OF_STORAGE_RANDOM_SETTINGS)

/** Settings for the StorageRandom engine.
  * Could be loaded from a CREATE RANDOM STREAM query (SETTINGS clause).
  */
struct StorageRandomSettings : public BaseSettings<StorageRandomSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

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
    UInt64 events_per_second;
    UInt64 interval_time;

protected:
    StorageRandom(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const String & comment,
        std::optional<UInt64> random_seed,
        UInt64 events_per_second_,
        UInt64 interval_time_);
};

}
