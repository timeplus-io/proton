#include <Storages/MatView/StorageMaterializedView.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/ProcessList.h>
#include <QueryPipeline/BlockIO.h>

#include <ranges>

namespace ProfileEvents
{
extern const Event UserTimeMicroseconds;
extern const Event SystemTimeMicroseconds;
}

namespace DB
{
Int64 StorageMaterializedView::getMetricTime(bool reset)
{
    if (reset)
        return pipeline_state.metric_time.exchange(MonotonicMilliseconds::now(), std::memory_order_relaxed);
    else
        return pipeline_state.metric_time.load(std::memory_order_relaxed);
}

UInt64 StorageMaterializedView::readBytes(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (reset)
        return pipeline_state.read_bytes.exchange(0, std::memory_order_relaxed);
    else
        return pipeline_state.read_bytes.load(std::memory_order_relaxed);
}

UInt64 StorageMaterializedView::readRows(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (reset)
        return pipeline_state.read_rows.exchange(0, std::memory_order_relaxed);
    else
        return pipeline_state.read_rows.load(std::memory_order_relaxed);
}

UInt64 StorageMaterializedView::writtenBytes(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (reset)
        return pipeline_state.written_bytes.exchange(0, std::memory_order_relaxed);
    else
        return pipeline_state.written_bytes.load(std::memory_order_relaxed);
}

UInt64 StorageMaterializedView::writtenRows(bool reset, [[maybe_unused]] bool external_ingress)
{
    if (reset)
        return pipeline_state.written_rows.exchange(0, std::memory_order_relaxed);
    else
        return pipeline_state.written_rows.load(std::memory_order_relaxed);
}

StorageMaterializedView::PipelineMetrics StorageMaterializedView::getPipelineMetrics() const
{
    PipelineMetrics metrics;

    /// https://github.com/timeplus-io/proton-enterprise/issues/6170
    /// When the query pipeline recovers from an exception,
    /// it resets the `BlockIO` and query context objects.
    /// Holding the `BlockIO` here delays the release of this object.
    /// However, the `QueryStatus` object owned by `BlockIO` holds a
    /// weak pointer to the query context. If it fails to acquire the
    /// query context pointer, it will throw a `LogicError`, causing
    /// a crash. Therefore, we need to manually extend the lifecycle
    /// of the query context object.
    auto query_context_holder = pipeline_state.query_context;
    auto block_io = pipeline_state.io.load();
    if (!block_io || !block_io->process_list_entry)
        return metrics;

    /// Get dynamic metrics only (memory and CPU) - skip static thread_list
    auto query_status_info = block_io->process_list_entry->getQueryStatus()->getInfo(
        /*get_thread_list=*/false, /*get_profile_events=*/true, /*get_settings=*/false);

    /// Memory usage (dynamic - changes constantly)
    metrics.memory_usage = query_status_info.memory_usage;

    /// Use cached static data (thread IDs)
    /// These are collected once when pipeline is built and never change
    metrics.thread_ids = pipeline_state.cached_thread_ids;

    /// CPU usage calculation (dynamic - real-time delta tracking)
    if (query_status_info.profile_counters && query_status_info.elapsed_microseconds > 0)
    {
        const auto & counters = *query_status_info.profile_counters;
        Int64 user_time = counters[ProfileEvents::UserTimeMicroseconds];
        Int64 system_time = counters[ProfileEvents::SystemTimeMicroseconds];
        Int64 current_cpu_microseconds = user_time + system_time;
        Int64 current_elapsed_microseconds = query_status_info.elapsed_microseconds;

        /// Get previous values for delta calculation
        Int64 prev_cpu = pipeline_state.last_cpu_microseconds.load(std::memory_order_relaxed);
        Int64 prev_elapsed = pipeline_state.last_elapsed_microseconds.load(std::memory_order_relaxed);

        /// Update stored values for next calculation
        pipeline_state.last_cpu_microseconds.store(current_cpu_microseconds, std::memory_order_relaxed);
        pipeline_state.last_elapsed_microseconds.store(current_elapsed_microseconds, std::memory_order_relaxed);

        /// Calculate delta-based CPU percentage (real-time usage)
        if (prev_elapsed > 0 && current_elapsed_microseconds > prev_elapsed)
        {
            Int64 cpu_delta = current_cpu_microseconds - prev_cpu;
            Int64 elapsed_delta = current_elapsed_microseconds - prev_elapsed;

            if (cpu_delta >= 0 && elapsed_delta > 0)
            {
                /// Real-time CPU percentage: 100% = 1 core, 200% = 2 cores, etc.
                metrics.cpu_usage_percentage = (static_cast<Float64>(cpu_delta) / static_cast<Float64>(elapsed_delta)) * 100.0;
            }
        }
    }

    return metrics;
}

UInt64 StorageMaterializedView::getCheckpointSize() const
{
    if (!curr_ckpt_ctx)
        return 0; /// Not initialized

    return Globals::getCheckpointCoordinator().getStorageSize(curr_ckpt_ctx);
}

PathSizes StorageMaterializedView::getCheckpointStat() const
{
    if (!curr_ckpt_ctx)
        return {}; /// Not initialized

    return Globals::getCheckpointCoordinator().getStorageStat(curr_ckpt_ctx);
}

CheckpointRequestMetricsPtr StorageMaterializedView::getCheckpointRequestMetrics() const
{
    if (!curr_ckpt_ctx)
        return nullptr; /// Not initialized

    return Globals::getCheckpointCoordinator().getCheckpointRequestMetrics(curr_ckpt_ctx);
}

Streaming::StreamingSourceMetricsPtrs StorageMaterializedView::getStreamingSourceMetrics() const
{
    /// If pipeline is active, collect metrics from running streaming sources
    if (const auto block_io = pipeline_state.io.load(); block_io)
    {
        Streaming::StreamingSourceMetricsPtrs sources_metrics;
        std::ranges::transform(block_io->pipeline.getStreamingSources(), std::back_inserter(sources_metrics), [](const auto & ss) {
            return ss->getMetrics();
        });
        return sources_metrics;
    }

    /// Otherwise, return the last stream source metrics collected in resetPipeline()
    /// For non-executing node, for example: current node became a follower after raft leader re-election, we still need output the last metrics once
    boost::shared_ptr<Streaming::StreamingSourceMetricsPtrs> last_metrics;
    if (isExecutingNode())
        last_metrics = pipeline_state.last_streaming_source_metrics.load();
    else
        last_metrics = pipeline_state.last_streaming_source_metrics.exchange(last_metrics);

    if (last_metrics)
        return *last_metrics;

    return {};
}

StorageMaterializedView::Metrics StorageMaterializedView::getMetrics() const
{
    /// Get all pipeline metrics in a single efficient call
    auto pipeline_metrics = getPipelineMetrics();

    return Metrics{
        .status = String{magic_enum::enum_name(getPipelineStatus())},
        .last_err_msg_and_ts = lastErrorMessageAndTimestamp(),
        .recover_times = getRetryTimes(),
        .memory_usage = pipeline_metrics.memory_usage,
        .cpu_usage_percentage = pipeline_metrics.cpu_usage_percentage,
        .thread_ids = std::move(pipeline_metrics.thread_ids),
        .ckpt_storage_size = getCheckpointSize(),
        .ckpt_request_metrics = getCheckpointRequestMetrics(),
        .source_metrics = getStreamingSourceMetrics(),
    };
}
}
