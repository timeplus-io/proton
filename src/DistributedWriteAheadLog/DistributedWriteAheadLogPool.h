#pragma once

#include "IDistributedWriteAheadLog.h"

#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Pooling DistributedWriteAheadLog. Singleton
/// The pool will be initied during system startup and will be read only after that.
/// So it doesn't hold any mutext in the multithread access env.
class DistributedWriteAheadLogPool : private boost::noncopyable
{
public:
    static DistributedWriteAheadLogPool & instance(ContextPtr global_context);

    explicit DistributedWriteAheadLogPool(ContextPtr global_context);
    ~DistributedWriteAheadLogPool();

    DistributedWriteAheadLogPtr get(const String & id) const;

    DistributedWriteAheadLogPtr getDefault() const;

    void startup();
    void shutdown();

private:
    void init(const String & key);

private:
    ContextPtr global_context;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    DistributedWriteAheadLogPtr default_wal;
    std::unordered_map<String, std::vector<DistributedWriteAheadLogPtr>> wals;
    mutable std::unordered_map<String, std::atomic_uint64_t> indexes;

    Poco::Logger * log;
};
}
