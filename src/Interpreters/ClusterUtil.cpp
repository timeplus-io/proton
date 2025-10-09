#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterUtil.h>
#include <Interpreters/Context.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/Cluster.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/MetaStore/MetaStore.h>


namespace DB
{
namespace ErrorCodes
{
extern const int NO_REMOTE_SHARD_AVAILABLE;
}

namespace
{

/// If current user is empty (for background MatView case), use special USER_INTERSERVER_MARKER as user and also get the cluster secret from the setup.
/// Otherwise use existing user and password in the current session and empty cluster secret
std::tuple<String, String, String> getInterClusterUserAndPassword(const ContextPtr & context)
{
    auto user = context->getUserName();
    if (user.empty())
        return std::tuple<String, String, String>{USER_INTERSERVER_MARKER, "", ""};
    else
        return std::tuple<String, String, String>{std::move(user), context->getPasswordForCurrentUser(), ""};
}

}

ClusterPtr getCluster(ContextPtr context, cluster::NodeID node_id)
{
    const auto & cluster = Globals::getMetaStore().getCluster();

    std::vector<std::vector<std::pair<String, cluster::NodeID>>> shards_addresses;
    shards_addresses.emplace_back();

    if (auto host_port = cluster.hostPortPublic(node_id))
        shards_addresses.back().emplace_back(host_port->hostPort(), node_id);

    if (shards_addresses.back().empty())
        throw Exception(ErrorCodes::NO_REMOTE_SHARD_AVAILABLE, "No hosts found for node_id=0x{}", node_id);

    const auto & [user, password, cluster_secret] = getInterClusterUserAndPassword(context);
    return std::make_shared<DB::Cluster>(
        context->getSettings(),
        shards_addresses,
        user,
        password,
        context->getClusterID(),
        cluster_secret,
        context->getNodeID(),
        DBMS_DEFAULT_PORT,
        /*treat_local_as_remote=*/false,
        /*treat_local_port_as_remote=*/context->getApplicationType() == Context::ApplicationType::LOCAL,
        /*secure=*/false);
}
}
