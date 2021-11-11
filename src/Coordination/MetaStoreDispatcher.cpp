#include "MetaStoreDispatcher.h"

#include <chrono>
#include <future>
#include <boost/algorithm/string.hpp>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/setThreadName.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

MetaStoreDispatcher::MetaStoreDispatcher()
    : coordination_settings(std::make_shared<CoordinationSettings>()), log(&Poco::Logger::get("MetaStoreDispatcher"))
{
}

void MetaStoreDispatcher::initialize(const Poco::Util::AbstractConfiguration & config, bool standalone_metastore)
{
    LOG_DEBUG(log, "Initializing metastore dispatcher");
    int myid = config.getInt("metastore_server.server_id");

    if (config.has("metastore_server.http_port"))
        http_port = config.getInt("metastore_server.http_port");
    if (config.has("metastore_server.tcp_port"))
        tcp_port = config.getInt("metastore_server.tcp_port");

    coordination_settings->loadFromConfig("metastore_server.coordination_settings", config);

    server = std::make_unique<MetaStoreServer>(myid, coordination_settings, config, snapshots_queue, standalone_metastore);
    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup();
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        server->waitInit();
        LOG_DEBUG(log, "Quorum initialized");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    LOG_DEBUG(log, "Dispatcher initialized");
}

void MetaStoreDispatcher::shutdown()
{
    try
    {
        if (shutdown_called)
            return;

        LOG_DEBUG(log, "Shutting down storage dispatcher");
        shutdown_called = true;

        if (server)
            server->shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

MetaStoreDispatcher::~MetaStoreDispatcher()
{
    shutdown();
}

String MetaStoreDispatcher::getLeaderHostname() const
{
    /// endpoint is "host:port", parts shall be ["host", "port"]
    std::vector<String> parts;
    const auto & endpoint = server->getClusterConfig()->get_server(server->getLeaderID())->get_endpoint();
    boost::split(parts, endpoint, boost::is_any_of(":"));
    if (parts.size() == 2)
        return parts[0];
    else
        return {};
}

}
