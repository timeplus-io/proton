#pragma once

#include <DistributedWALClient/KafkaWAL.h>
#include <Common/ThreadPool.h>

#include <boost/noncopyable.hpp>

#include <optional>

namespace DB
{
class Context;

class MetadataService : private boost::noncopyable
{

public:
    MetadataService(const ContextMutablePtr & global_context_, const String & service_name);
    virtual ~MetadataService();
    void startup();
    void shutdown();

    virtual bool ready() const { return started.test(); }

    const String & nodeRoles() const { return node_roles; }
    bool hasCurrentRole() const { return node_roles.find(role()) != String::npos; }
    std::vector<DWAL::KafkaWALClusterPtr> clusters();

private:
    void initPorts();
    void tailingRecords();
    void doTailingRecords();

    virtual void postStartup() { started.test_and_set(); }
    virtual void preShutdown() {}

    virtual void processRecords(const DWAL::RecordPtrs & records) = 0;

    virtual String role() const = 0;
    virtual String cleanupPolicy() const { return "delete"; }

    virtual std::pair<Int32, Int32> batchSizeAndTimeout() const { return std::make_pair(100, 500); }

    /// Create DWal on server
    void waitUntilDWalReady(const DWAL::KafkaWALContext & ctx) const;

protected:
    void setupRecordHeaders(DWAL::Record & record, const String & version) const;

    void doCreateDWal(const DWAL::KafkaWALContext & ctx) const;
    void doDeleteDWal(const DWAL::KafkaWALContext & ctx) const;

    DWAL::AppendResult appendRecord(const DWAL::Record & record) const
    {
        /// A centralized place to append record for metadata services
        /// for easy record checking
        assert(!record.hasSchema());
        return dwal->append(record, dwal_append_ctx);
    }

protected:
    struct ConfigSettings
    {
        String key_prefix;

        /// Topic
        String default_name;
        Int32 default_data_retention;

        /// Producer
        Int32 request_required_acks = 1;
        Int32 request_timeout_ms = 10000;

        /// Consumer
        String auto_offset_reset = "earliest";
        Int64 initial_default_offset = -2;
    };
    virtual ConfigSettings configSettings() const = 0;

protected:
    ContextMutablePtr global_context;

    DWAL::KafkaWALContext dwal_append_ctx;
    DWAL::KafkaWALContext dwal_consume_ctx;
    DWAL::KafkaWALPtr dwal;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
    std::atomic_flag started = ATOMIC_FLAG_INIT;
    std::optional<ThreadPool> pool;

    String node_roles;
    Int16 https_port = -1;
    Int16 http_port = -1;
    Int16 tcp_port = -1;
    Int16 tcp_port_secure = -1;

    Poco::Logger * log;
};
}
