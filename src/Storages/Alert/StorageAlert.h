#pragma once

#include <Cluster/Protocol/AlertDescriptor.h>
#include <IO/WriteHelpers.h>
#include <QueryPipeline/BlockIO.h>
#include <Storages/IStorage.h>
#include <Common/ThreadPool.h>

#include <boost/smart_ptr/atomic_shared_ptr.hpp>

namespace DB
{

class StorageAlert final : public IStorage, WithMutableContext
{
public:
    class ShardSNs final : public std::unordered_map<size_t /*shard*/, Int64 /*sn*/>
    {
    public:
        [[nodiscard]] String toString() const
        {
            bool first{true};
            WriteBufferFromOwnString buf;
            writeChar('{', buf);
            for (const auto [k, v] : *this)
            {
                if (first)
                    first = false;
                else
                    writeString(", ", buf);
                writeIntText(k, buf);
                writeString(": ", buf);
                writeIntText(v, buf);
            }
            writeChar('}', buf);
            return buf.str();
        }
    };

    struct TriggerRecord
    {
        ShardSNs sns;
        UInt64 timestamp{0};
        UInt64 rows{0};
    };

    struct Metrics
    {
        std::atomic_uint64_t trigger_count{0};
        std::atomic_uint64_t throttle_count{0};
        std::atomic_uint64_t error_count{0};
    };

    enum class ValidationLevel : uint8_t
    {
        Full = 0,
        Quick = 1,
        None = 2,
    };

    StorageAlert(StorageID, cluster::protocol::AlertDescriptorPtr, ValidationLevel, ContextMutablePtr);
    ~StorageAlert() override;

    static constexpr auto name = "StorageAlert";
    std::string getName() const override { return name; }

    void startup() override;
    void shutdown(bool dropping) override;


    String getStatus() const;
    Int64 getLastThrottleTimestamp() const;
    TriggerRecord getLastTrigger();
    std::pair<Int64, String> getLastError();
    const Metrics & getMetrics() const;

private:
    enum class Stage : uint8_t
    {
        Unknown = 0,
        Build = 1,
        Execute = 2,
        Error = 3,
        Failed = 4,
        Stopped = 5,
    };

    void validateSelectQuery(bool quick);

    QueryPipeline buildQueryPipelineImpl(ASTPtr inner_query);

    void checkDependencies();
    void run();

    void tryBuildBackgroundPipeline();
    void buildBackgroundPipeline();

    void tryExecuteBackgroundPipeline();
    void executeBackgroundPipeline();

    void setPipelineExeception(Exception &&);

    void markSNsWithoutLock();
    void checkpoint() noexcept;
    /// Returns shard:sn map
    ShardSNs recover() noexcept;
    void recoverFromCheckpoints();

    static constexpr size_t internal_recheck_interval_ms = 100;

    cluster::protocol::AlertDescriptorPtr desc;
    SelectQueryDescription select_query;
    String query_id;
    bool wait_for_dependencies{false};
    ContextMutablePtr query_context;

    StoragePtr storage_udf;
    boost::atomic_shared_ptr<BlockIO> io;
    std::vector<std::shared_ptr<Streaming::ISource>> pipeline_sources;
    ThreadFromGlobalPool executor;

    std::atomic_flag started;
    std::atomic_flag stopped;
    std::atomic_flag shut;
    std::atomic<Stage> pipeline_stage{Stage::Unknown};

    std::mutex pipeline_exception_mutux;
    std::optional<Exception> pipeline_exception;
    Int64 pipeline_exception_ts{-1};

    std::mutex trigger_mutex;
    TriggerRecord last_trigger;

    std::atomic_int64_t last_throttled_ts{-1};
    Metrics metrics;

    LoggerPtr logger;
};

}
