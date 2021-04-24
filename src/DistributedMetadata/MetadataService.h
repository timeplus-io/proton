#pragma once

#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

#include <any>
#include <optional>


namespace DB
{
class Context;

class MetadataService : private boost::noncopyable
{

public:
    MetadataService(const ContextPtr & global_context_, const String & service_name);
    virtual ~MetadataService();
    void startup();
    void shutdown();

private:
    void tailingRecords();
    void doTailingRecords();
    virtual void postStartup() {}
    virtual void preShutdown() {}
    virtual void processRecords(const IDistributedWriteAheadLog::RecordPtrs & records) = 0;
    virtual String role() const = 0;
    virtual String cleanupPolicy() const { return "delete"; }
    virtual std::pair<Int32, Int32> batchSizeAndTimeout() const { return std::make_pair(100, 500); }

    /// Create DWal on server
    void createDWal();
    void waitUntilDWalReady(std::any & ctx);

protected:
    void doCreateDWal(std::any & ctx);
    void doDeleteDWal(std::any & ctx);

protected:
    struct ConfigSettings
    {
        String name_key;
        /// Topic
        String default_name;
        String data_retention_key;
        Int32 default_data_retention;
        String replication_factor_key;
        /// Producer
        Int32 request_required_acks = 1;
        Int32 request_timeout_ms = 10000;
        /// Consumer
        String auto_offset_reset = "earliest";
        Int64 initial_default_offset = -2;
    };
    virtual ConfigSettings configSettings() const = 0;

protected:
    ContextPtr global_context;

    std::any dwal_append_ctx;
    std::any dwal_consume_ctx;
    DistributedWriteAheadLogPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> pool;

    Poco::Logger * log;
};
}
