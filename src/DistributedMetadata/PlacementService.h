#pragma once

#include "MetadataService.h"
#include "PlacementStrategy.h"

#include <Core/BackgroundSchedulePool.h>

namespace DB
{
class CatalogService;

class PlacementService final : public MetadataService
{
public:
    static PlacementService & instance(const ContextMutablePtr & global_context);

    explicit PlacementService(const ContextMutablePtr & global_context_);
    PlacementService(const ContextMutablePtr & global_context_, PlacementStrategyPtr strategy_);
    virtual ~PlacementService() override
    {
        preShutdown();
    }

    void scheduleBroadcast();

    std::vector<NodeMetricsPtr>
    place(size_t shards, size_t replication_factor, const String & storage_policy = "default", const String & colocated_table = "") const;

    std::vector<String> placed(const String & database, const String & table) const;

    String getNodeIdentityByChannel(const String & channel) const;
    std::vector<NodeMetricsPtr> nodes() const;

    bool ready() const override { return started.test() && nodes().size(); }
    void preShutdown();

private:
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

private:
    CatalogService & catalog;

    mutable std::shared_mutex rwlock;
    NodeMetricsContainer nodes_metrics;

    PlacementStrategyPtr strategy;
    BackgroundSchedulePoolTaskHolder broadcast_task;

    size_t reschedule_interval;
};

}
