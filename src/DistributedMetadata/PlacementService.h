#pragma once

#include "MetadataService.h"
#include "PlacementStrategy.h"

#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
struct StorageInfoForStream
{
    StorageID id = StorageID::createEmpty();
    uint64_t streaming_data_bytes = 0;
    std::vector<String> streaming_data_paths;
    uint64_t historical_data_bytes = 0;
    std::vector<String> historical_data_paths;

    Poco::Dynamic::Var toJSON(bool is_simple = false) const;
};

struct StreamStorageInfo
{
    uint64_t total_bytes_on_disk = 0;
    std::unordered_map<String, StorageInfoForStream> streams;
    std::optional<bool> need_sort_by_bytes; /// {}: not sort, true: desc sort, false: asc sort

    Poco::Dynamic::Var toJSON(bool is_simple = false) const;
    void sortingStreamByBytes(bool desc = true) { need_sort_by_bytes = desc; }
};
using StreamStorageInfoPtr = std::shared_ptr<StreamStorageInfo>;

class CatalogService;

class PlacementService final : public MetadataService
{
public:
    static PlacementService & instance(const ContextMutablePtr & global_context);

    explicit PlacementService(const ContextMutablePtr & global_context_);
    PlacementService(const ContextMutablePtr & global_context_, PlacementStrategyPtr strategy_);
    virtual ~PlacementService() override { preShutdown(); }

    void scheduleBroadcast();

    std::vector<NodeMetricsPtr>
    place(size_t shards, size_t replication_factor, const String & storage_policy = "default", const String & colocated_table = "") const;

    std::vector<String> placed(const String & database, const String & table) const;

    String getNodeIdentityByChannel(const String & channel) const;
    std::vector<NodeMetricsPtr> nodes() const;

    bool ready() const override { return started.test() && nodes().size(); }

    StreamStorageInfoPtr loadStorageInfo(const String & ns, const String & stream);
    void checkStorageQuotaOrThrow();

private:
    void preShutdown();
    void processRecords(const nlog::RecordPtrs & records) override;
    String role() const override { return "placement"; }
    String cleanupPolicy() const override { return "compact"; }
    ConfigSettings configSettings() const override;
    std::pair<Int32, Int32> batchSizeAndTimeout() const override { return std::make_pair(100, 500); }

private:
    void mergeMetrics(const String & key, const nlog::Record & record);

    /// `broadcast` broadcasts the metrics of this node
    void broadcast();
    void doBroadcast();

    void loadLocalStoragesInfo(StreamStorageInfoPtr & disk_info, const String & ns, const String & stream) const;
    std::pair<size_t, std::vector<String>> getLocalStorageInfo(StoragePtr storage) const;

private:
    CatalogService & catalog;

    mutable std::shared_mutex rwlock;
    NodeMetricsContainer nodes_metrics;

    PlacementStrategyPtr strategy;
    BackgroundSchedulePoolTaskHolder broadcast_task;

    size_t reschedule_interval;
    StreamStorageInfoPtr storage_info_ptr = nullptr;
    uint64_t last_update_s;
};

}
