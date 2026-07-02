#include <Cluster/Common/AppliedSequence.h>
#include <Cluster/Common/Cluster.h>
#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/MetaStore/ProposalRegistry.h>

#include <Common/CurrentMetrics.h>

#include <base/sleep.h>
#include <fmt/ranges.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace cluster::meta
{

MetaStore::MetaStore(StoreConfigPtr config_, std::shared_ptr<MetaDB> meta_db_)
    : config(std::move(config_))
    , meta_db(std::move(meta_db_))
    , server_descriptor(nullptr)
    , cluster("cluster")
    , logger(getLogger("MetaStore"))
{
    /// Always node ID 1
    node_identity = 1;

    /// Load cached UDF names early to avoid races after readiness
    loadUserDefinedFunctions();
}

MetaStore::~MetaStore() noexcept
{
    shutdown();
    LOG_INFO(logger, "MetaStore destroyed");
}

void MetaStore::startup()
{
    if (started.test_and_set())
        return;

    LOG_INFO(logger, "Starting MetaStore");

    init();

    if (meta_db)
    {
        timer.emplace(/*thread_pool_size=*/1, /*tick_ms=*/1000, /*wheel_size=*/3600);
        auto retention_interval_ms = std::max<int64_t>(5000, config->retention_check_ms);
        auto timer_task = timer->add(
            retention_interval_ms,
            [this]() {
                try
                {
                    /// Get the current applied sequence number
                    auto applied_sn_result = meta_db->loadAppliedSequence();
                    if (!applied_sn_result.hasError())
                    {
                        /// Keep a tail of recent requests for safety (e.g., last 100)
                        constexpr int64_t KEEP_TAIL = 100;
                        uint64_t cleanup_sn
                            = applied_sn_result.result.sn > KEEP_TAIL ? static_cast<uint64_t>(applied_sn_result.result.sn - KEEP_TAIL) : 0;

                        if (cleanup_sn > 0)
                        {
                            auto cleanup_err = meta_db->cleanupOldPendingRequests(cleanup_sn);
                            if (cleanup_err.hasError())
                            {
                                LOG_WARNING(logger, "Failed to cleanup old pending requests: {}", cleanup_err.string());
                            }
                        }
                    }
                }
                catch (...)
                {
                    DB::tryLogCurrentException(logger, "Failed to run metadata retention cleanup");
                }
            },
            /*repeat=*/true);

        if (!timer_task)
            LOG_ERROR(logger, "Failed to schedule metadata retention cleanup, SystemTimer is slow");

        /// Start the timer polling loop in a separate thread
        timer_looper.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1);
        timer_looper->scheduleOrThrowOnError([this]() {
            setThreadName("TimerPoller");
            loopTimer();
        });

        LOG_DEBUG(logger, "Started timer polling loop");
    }
    LOG_DEBUG(logger, "MetaStore started successfully");
}

void MetaStore::shutdown()
{
    bool expected = false;
    if (!stopped.compare_exchange_strong(expected, true))
        return; /// Already stopped

    LOG_INFO(logger, "Shutting down MetaStore");

    ready = false;

    if (timer_looper)
    {
        timer_looper->wait();
        timer_looper.reset();
    }

    if (timer)
    {
        timer->wait();
        timer.reset();
    }

    /// Mark clean shutdown
    writeCleanShutdownFile();

    LOG_INFO(logger, "MetaStore shutdown complete");
}

void MetaStore::init()
{
    LOG_DEBUG(logger, "Initializing MetaStore with metadata role at {}", storeDir().string());

    bool was_clean_shutdown = removeCleanShutdownFile();
    if (!was_clean_shutdown)
        LOG_INFO(logger, "Previous shutdown was not clean, may need recovery");

    if (!local_meta_queue)
    {
        LOG_WARNING(logger, "LocalMetaQueue not set from bootstrap, creating default");
        local_meta_queue = std::make_shared<LocalMetaQueue>(meta_db, logger);
    }
    if (!proposal_registry)
    {
        LOG_WARNING(logger, "ProposalRegistry not set from bootstrap, creating default");
        proposal_registry = std::make_shared<ProposalRegistry>(logger);
    }

    /// Mark as ready
    setReady();

    LOG_DEBUG(logger, "MetaStore initialization complete");
}

/// Proposal acknowledgment
void MetaStore::ackProposal(uint64_t correlation_id, int64_t /*sn*/, int error_code, String && error_message)
{
    /// Skip notification for recovered entries (correlation_id=0)
    /// After restart, all client connections are gone anyway
    if (correlation_id == 0)
    {
        LOG_DEBUG(logger, "Skipping ackProposal for recovered entry (correlation_id=0)");
        return;
    }

    /// Complete the proposal in the registry
    if (proposal_registry)
    {
        if (error_code != 0)
            proposal_registry->cancelProposal(correlation_id, Error(error_code, std::move(error_message)));
        else
            proposal_registry->completeProposal(correlation_id);
    }
}

CallResultV<AppliedSequence> MetaStore::appliedSequence(const cluster::StreamIDShard &) const
{
    return CallResultV<AppliedSequence>{AppliedSequence{}};
}

bool MetaStore::isLocalNode(
    [[maybe_unused]] const String & host, [[maybe_unused]] uint16_t port, [[maybe_unused]] const String & fqdn_name) const
{
    /// Always true (only one node)
    return true;
}

void MetaStore::loopTimer()
{
    constexpr size_t timer_poll_timeout_ms = 2000;
    constexpr size_t timer_processing_threshold = 2 * timer_poll_timeout_ms;

    Stopwatch stopwatch;

    while (!stopped.load(std::memory_order_relaxed))
    {
        stopwatch.restart();

        auto [reinserted, expired] = timer->poll(timer_poll_timeout_ms);

        auto elapsed = stopwatch.elapsedMilliseconds();
        if (elapsed >= timer_processing_threshold)
            LOG_WARNING(logger, "Took {}ms to handle timers, reinserted={} expired={}", elapsed, reinserted, expired);
    }
}
}
