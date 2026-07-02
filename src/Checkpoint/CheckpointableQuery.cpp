#include <Checkpoint/CheckpointableQuery.h>

#include <Checkpoint/CheckpointConfig.h>
#include <Checkpoint/CheckpointStorage.h>

#include <Bootstrap/Globals.h>
#include <Core/ULID.h>
#include <fmt/ranges.h>

namespace DB
{

CheckpointableQuery::CheckpointableQuery(
    const CheckpointStorage & storage_,
    CheckpointSettingsPtr settings_,
    std::weak_ptr<PipelineExecutor> executor_,
    std::optional<CheckpointEpoch> recovered_epoch,
    CheckpointContextPtr ctx_)
    : storage(storage_), settings(std::move(settings_)), executor(std::move(executor_)), ctx(std::move(ctx_))
{
    auto ack_node_descs{executor->getExecGraph().checkpointAckNodeDescriptions()};
    auto stateful_node_descs{executor->getExecGraph().statefulNodeDescriptions()};
    ack_nodes_readonly.reserve(ack_node_descs.size() + stateful_node_descs.size());

    for (const auto & desc : ack_node_descs)
        ack_nodes_readonly.emplace(desc.logic_pid, desc.string());

    /// Also track the ack status of all stateful nodes
    for (const auto & desc : stateful_node_descs)
        ack_nodes_readonly.emplace(desc.logic_pid, desc.string());

    chassert(!ack_nodes_readonly.empty());

    if (recovered_epoch)
        current_epoch = *recovered_epoch;

    prepareForNextEpoch();

    /// Only have stateful sources can be considered a lightweight checkpoint (such as ETL).
    is_lightweight = std::ranges::none_of(executor->getProcessors(), [](const auto & processor) {
        return processor->isStreaming() && processor->hasState() && !processor->isSource();
    });
}

bool CheckpointableQuery::ack(UInt32 node_id)
{
    ack_nodes.erase(node_id);
    return ack_nodes.empty();
}

void CheckpointableQuery::prepareForNextEpoch()
{
    chassert(current_epoch > last_epoch);
    last_epoch = current_epoch;
    current_epoch.reset();
    ack_nodes = ack_nodes_readonly;
}

CheckpointContextPtr CheckpointableQuery::prepareNextCheckpointContext(std::function<void(CheckpointContextPtr)> && callback)
{
    auto ckpt_request_ctx = std::make_shared<CheckpointRequestContext>();
    if (current_epoch.empty())
    {
        /// NOTE: the source processor to start checkpoint in a new epoch
        ckpt_request_ctx->executor = executor;
        current_epoch = CheckpointEpoch{.epoch = last_epoch.epoch + 1, .ulid = ULIDHelpers().generate()};
    }
    ckpt_request_ctx->last_epoch = last_epoch;
    ckpt_request_ctx->callback = std::move(callback);
    ckpt_request_ctx->settings = settings;
    ckpt_request_ctx->metrics = std::make_shared<CheckpointRequestMetrics>();
    return ctx->cloneWithEpoch(current_epoch, std::move(ckpt_request_ctx));
}

UInt64 CheckpointableQuery::checkpointInterval(const CheckpointConfig & config) const
{
    /// Case-1: Use the specified interval if present
    if (settings->interval)
        return settings->interval;

    UInt64 default_interval = is_lightweight ? config.light_state_interval_sec : config.min_interval_sec;

    if (metrics)
    {
        /// The time overhead of checkpoint requests at different levels should be less than or equal to 1/10 of the current interval.
        /// Otherwise, the interval is considered overloaded and needs to be adjusted based on the elapsed time.
        auto elapsed_ms = metrics->stopwatch.elapsedMilliseconds();
        auto total_bytes = metrics->total_bytes.load();
        if (total_bytes > config.heavy_state_size_threshold)
            /// Case-2: Heavy checkpoint request (e.g., > 500MB). Examples (heavy_state_interval=900s):
            /// 1. elapsed=1s, interval=15m
            /// 2. elapsed=10s, interval=15m
            /// 3. elapsed=1m, interval=15m
            /// 4. elapsed=2m, interval=20m
            /// 5. elapsed=3m, interval=30m
            return std::max(config.heavy_state_interval, elapsed_ms / 100);
        else
            /// Case-3: Normal checkpoint request (e.g., <= 500MB). Examples:
            /// 1. elapsed=100ms, interval=5s
            /// 2. elapsed=500ms, interval=5s
            /// 3. elapsed=800ms, interval=8s
            /// 4. elapsed=1s, interval=10s
            /// 5. elapsed=3s, interval=30s
            return std::max(default_interval, elapsed_ms / 100);
    }

    /// Case-4: Assume it's a normal checkpoint request if no metrics
    return default_interval;
}

String CheckpointableQuery::checkpointAckNodeDescriptions() const
{
    std::vector<std::string_view> node_descs;
    node_descs.reserve(ack_nodes_readonly.size());
    for (const auto & [_, desc] : ack_nodes_readonly)
        node_descs.emplace_back(desc);

    return fmt::format("{}", fmt::join(node_descs, ", "));
}
}
