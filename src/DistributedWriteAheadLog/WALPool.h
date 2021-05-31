#pragma once

#include "Cluster.h"
#include "WAL.h"

#include <Interpreters/Context_fwd.h>

namespace DB
{
namespace DWAL
{
/// Pooling WAL. Singleton
/// The pool will be initied during system startup and will be read only after that.
/// So it doesn't hold any mutext in the multithread access env.
class WALPool : private boost::noncopyable
{
public:
    static WALPool & instance(ContextPtr global_context);

    explicit WALPool(ContextPtr global_context);
    ~WALPool();

    WALPtr get(const String & id) const;

    WALPtr getMeta() const;

    std::vector<ClusterPtr> clusters(std::any & ctx) const;

    void startup();
    void shutdown();

private:
    void init(const String & key);

private:
    ContextPtr global_context;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    String default_cluster = "";
    WALPtr meta_wal;
    std::unordered_map<String, std::vector<WALPtr>> wals;
    mutable std::unordered_map<String, std::atomic_uint64_t> indexes;

    Poco::Logger * log;
};
}
}
