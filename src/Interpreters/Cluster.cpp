#include <Core/Settings.h>
#include <Interpreters/Cluster.h>
#include <base/range.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/DNSResolver.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/parseAddress.h>

#include <Poco/Util/Application.h>

#include <span>


namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_ELEMENT_IN_CONFIG;
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
extern const int LOGICAL_ERROR;
extern const int SHARD_HAS_NO_CONNECTIONS;
extern const int SYNTAX_ERROR;
}

namespace
{

/// Default shard weight.
constexpr UInt32 default_weight = 1;
}

Cluster::Address::Address(
    const String & host_port_,
    cluster::NodeID node_id_,
    const String & user_,
    const String & password_,
    const String & cluster_id_,
    const String & cluster_secret_,
    UInt16 default_tcp_port,
    bool treat_local_port_as_remote,
    bool is_local_,
    bool secure_,
    Int64 priority_,
    UInt32 shard_index_,
    UInt32 replica_index_)
    : node_id(node_id_), user(user_), password(password_), cluster(cluster_id_), cluster_secret(cluster_secret_)
{
    bool can_be_local = true;
    std::pair<std::string, UInt16> parsed_host_port;
    if (!treat_local_port_as_remote)
    {
        parsed_host_port = parseAddress(host_port_, default_tcp_port);
    }
    else
    {
        /// For clickhouse-local (treat_local_port_as_remote) try to read the address without passing a default port
        /// If it works we have a full address that includes a port, which means it won't be local
        /// since clickhouse-local doesn't listen in any port
        /// If it doesn't include a port then use the default one and it could be local (if the address is)
        try
        {
            parsed_host_port = parseAddress(host_port_, 0);
            can_be_local = false;
        }
        catch (...)
        {
            parsed_host_port = parseAddress(host_port_, default_tcp_port);
        }
    }

    host_name = parsed_host_port.first;
    port = parsed_host_port.second;
    secure = secure_ ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    priority = priority_;
    is_local = can_be_local && is_local_;
    shard_index = shard_index_;
    replica_index = replica_index_;
}

String Cluster::Address::toString() const
{
    return toString(host_name, port);
}

String Cluster::Address::toString(const String & host_name, UInt16 port)
{
    return fmt::format("{}:{}", escapeForFileName(host_name), port);
}

String Cluster::Address::readableString() const
{
    String res;

    /// If it looks like IPv6 address add braces to avoid ambiguity in ipv6_host:port notation
    if (host_name.find_first_of(':') != std::string::npos && !host_name.empty() && host_name.back() != ']')
        res += fmt::format("[{}]", host_name);
    else
        res += host_name;

    res += fmt::format(":{}", port);
    return res;
}

std::pair<String, UInt16> Cluster::Address::fromString(const String & host_port_string)
{
    auto pos = host_port_string.find_last_of(':');
    if (pos == std::string::npos)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect <host>:<port> format {}", host_port_string);

    return {unescapeForFileName(host_port_string.substr(0, pos)), parse<UInt16>(host_port_string.substr(pos + 1))};
}

String Cluster::Address::toFullString(bool use_compact_format) const
{
    if (use_compact_format)
    {
        if (shard_index == 0 || replica_index == 0)
            // shard_num/replica_num like in system.clusters table
            throw Exception(ErrorCodes::LOGICAL_ERROR, "shard_num/replica_num cannot be zero");

        return fmt::format("shard{}_replica{}", shard_index, replica_index);
    }
    else
    {
        return escapeForFileName(user) + (password.empty() ? "" : (':' + escapeForFileName(password))) + '@' + escapeForFileName(host_name)
            + ':' + std::to_string(port) + (default_database.empty() ? "" : ('#' + escapeForFileName(default_database)))
            + ((secure == Protocol::Secure::Enable) ? "+secure" : "");
    }
}

Cluster::Address Cluster::Address::fromFullString(const String & full_string)
{
    const char * address_begin = full_string.data();
    const char * address_end = address_begin + full_string.size();

    const char * user_pw_end = strchr(full_string.data(), '@');

    /// parsing with the new shard{shard_index}[_replica{replica_index}] format
    if (!user_pw_end && startsWith(full_string, "shard"))
    {
        const char * underscore = strchr(full_string.data(), '_');

        Address address;
        address.shard_index = parse<UInt32>(address_begin + strlen("shard"));
        address.replica_index = underscore ? parse<UInt32>(underscore + strlen("_replica")) : 0;

        return address;
    }
    else
    {
        /// parsing with the old user[:password]@host:port#default_database format
        /// This format is appeared to be inconvenient for the following reasons:
        /// - credentials are exposed in file name;
        /// - the file name can be too long.

        Protocol::Secure secure = Protocol::Secure::Disable;
        const char * secure_tag = "+secure";
        if (endsWith(full_string, secure_tag))
        {
            address_end -= strlen(secure_tag);
            secure = Protocol::Secure::Enable;
        }

        const char * colon = strchr(full_string.data(), ':');
        if (!user_pw_end || !colon)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect user[:password]@host:port#default_database format {}", full_string);

        const bool has_pw = colon < user_pw_end;
        const char * host_end = has_pw ? strchr(user_pw_end + 1, ':') : colon;
        if (!host_end)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Incorrect address '{}', it does not contain port", full_string);

        const char * has_db = strchr(full_string.data(), '#');
        const char * port_end = has_db ? has_db : address_end;

        Address address;
        address.secure = secure;
        address.port = parse<UInt16>(host_end + 1, port_end - (host_end + 1));
        address.host_name = unescapeForFileName(std::string(user_pw_end + 1, host_end));
        address.user = unescapeForFileName(std::string(address_begin, has_pw ? colon : user_pw_end));
        address.password = has_pw ? unescapeForFileName(std::string(colon + 1, user_pw_end)) : std::string();
        address.default_database = has_db ? unescapeForFileName(std::string(has_db + 1, address_end)) : std::string();
        // address.priority ignored
        return address;
    }
}

const Cluster::ShardInfo & Cluster::getAnyShardInfo() const
{
    if (shards_info.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster is empty");

    return shards_info.front();
}

Cluster::Cluster(
    const Settings & settings,
    const std::vector<std::vector<std::pair<String, cluster::NodeID>>> & host_addrs,
    const String & username,
    const String & password,
    const String & cluster_id,
    const String & cluster_secret,
    cluster::NodeID current_node_id,
    UInt16 default_tcp_port,
    bool treat_local_as_remote,
    bool treat_local_port_as_remote,
    bool secure,
    Int64 priority)
{
    UInt32 current_shard_num = 1;

    for (const auto & shard_host_addrs : host_addrs)
    {
        Addresses current;
        for (const auto & [replica_host, node_id] : shard_host_addrs)
            current.emplace_back(
                replica_host,
                node_id,
                username,
                password,
                cluster_id,
                cluster_secret,
                default_tcp_port,
                treat_local_port_as_remote,
                /*is_local_=*/current_node_id == node_id,
                secure,
                priority,
                current_shard_num,
                current.size() + 1);

        addresses_with_failover.emplace_back(current);

        Addresses shard_local_addresses;
        Addresses all_addresses;
        ConnectionPoolPtrs all_replicas;
        all_replicas.reserve(current.size());

        for (const auto & replica : current)
        {
            auto replica_pool = ConnectionPoolFactory::instance().get(
                static_cast<unsigned>(settings.distributed_connections_pool_size),
                replica.host_name,
                replica.port,
                replica.default_database,
                replica.user,
                replica.password,
                replica.quota_key,
                replica.cluster,
                replica.cluster_secret,
                "server",
                replica.compression,
                replica.secure,
                replica.priority);
            all_replicas.emplace_back(replica_pool);
            if (replica.is_local && !treat_local_as_remote)
                shard_local_addresses.push_back(replica);
            all_addresses.push_back(replica);
        }

        ConnectionPoolWithFailoverPtr shard_pool = std::make_shared<ConnectionPoolWithFailover>(
            all_replicas,
            settings.load_balancing,
            settings.distributed_replica_error_half_life.totalSeconds(),
            settings.distributed_replica_error_cap);

        slot_to_shard.insert(std::end(slot_to_shard), default_weight, shards_info.size());
        shards_info.push_back({
            /*insert_path_for_internal_replication=*/{},
            current_shard_num,
            default_weight,
            std::move(shard_local_addresses),
            std::move(all_addresses),
            std::move(shard_pool),
            std::move(all_replicas),
            false // has_internal_replication
        });
        ++current_shard_num;
    }

    initMisc();
}

Poco::Timespan Cluster::saturate(Poco::Timespan v, Poco::Timespan limit)
{
    if (limit.totalMicroseconds() == 0)
        return v;
    else
        return (v > limit) ? limit : v;
}


void Cluster::initMisc()
{
    for (const auto & shard_info : shards_info)
    {
        if (!shard_info.isLocal() && !shard_info.hasRemoteConnections())
            throw Exception(ErrorCodes::SHARD_HAS_NO_CONNECTIONS, "Found shard without any specified connection");
    }

    for (const auto & shard_info : shards_info)
    {
        if (shard_info.isLocal())
            ++local_shard_count;
        else
            ++remote_shard_count;
    }
}

std::unique_ptr<Cluster> Cluster::getClusterWithReplicasAsShards(const Settings & settings, size_t max_replicas_from_shard) const
{
    return std::unique_ptr<Cluster>{new Cluster(ReplicasAsShardsTag{}, *this, settings, max_replicas_from_shard)};
}

std::unique_ptr<Cluster> Cluster::getClusterWithSingleShard(size_t index) const
{
    return std::unique_ptr<Cluster>{new Cluster(SubclusterTag{}, *this, {index})};
}

std::unique_ptr<Cluster> Cluster::getClusterWithMultipleShards(const std::vector<UInt64> & indices) const
{
    return std::unique_ptr<Cluster>{new Cluster(SubclusterTag{}, *this, indices)};
}

namespace
{

void shuffleReplicas(std::vector<Cluster::Address> & replicas, const Settings & settings, size_t replicas_needed)
{
    std::random_device rd;
    std::mt19937 gen{rd()};

    if (settings.prefer_localhost_replica)
    {
        // force for local replica to always be included
        auto first_non_local_replica
            = std::partition(replicas.begin(), replicas.end(), [](const auto & replica) { return replica.is_local; });
        size_t local_replicas_count = first_non_local_replica - replicas.begin();

        if (local_replicas_count == replicas_needed)
        {
            /// we have exact amount of local replicas as needed, no need to do anything
            return;
        }

        if (local_replicas_count > replicas_needed)
        {
            /// we can use only local replicas, shuffle them
            std::shuffle(replicas.begin(), first_non_local_replica, gen);
            return;
        }

        /// shuffle just non local replicas
        std::shuffle(first_non_local_replica, replicas.end(), gen);
        return;
    }

    std::shuffle(replicas.begin(), replicas.end(), gen);
}

}

Cluster::Cluster(Cluster::ReplicasAsShardsTag, const Cluster & from, const Settings & settings, size_t max_replicas_from_shard)
{
    if (from.addresses_with_failover.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster is empty");

    UInt32 shard_num = 0;
    std::set<std::pair<String, int>> unique_hosts;
    for (size_t shard_index : collections::range(0, from.shards_info.size()))
    {
        auto create_shards_from_replicas = [&](std::span<const Address> replicas) {
            for (const auto & address : replicas)
            {
                if (!unique_hosts.emplace(address.host_name, address.port).second)
                    continue; /// Duplicate host, skip.

                ShardInfo info;
                info.shard_num = ++shard_num;

                if (address.is_local)
                    info.local_addresses.push_back(address);

                info.all_addresses.push_back(address);

                auto pool = ConnectionPoolFactory::instance().get(
                    static_cast<unsigned>(settings.distributed_connections_pool_size),
                    address.host_name,
                    address.port,
                    address.default_database,
                    address.user,
                    address.password,
                    address.quota_key,
                    address.cluster,
                    address.cluster_secret,
                    "server",
                    address.compression,
                    address.secure,
                    address.priority);

                info.pool = std::make_shared<ConnectionPoolWithFailover>(ConnectionPoolPtrs{pool}, settings.load_balancing);
                info.per_replica_pools = {std::move(pool)};

                addresses_with_failover.emplace_back(Addresses{address});
                shards_info.emplace_back(std::move(info));
            }
        };

        const auto & replicas = from.addresses_with_failover[shard_index];
        if (!max_replicas_from_shard || replicas.size() <= max_replicas_from_shard)
        {
            create_shards_from_replicas(replicas);
        }
        else
        {
            auto shuffled_replicas = replicas;
            // shuffle replicas so we don't always pick the same subset
            shuffleReplicas(shuffled_replicas, settings, max_replicas_from_shard);
            create_shards_from_replicas(std::span{shuffled_replicas.begin(), max_replicas_from_shard});
        }
    }

    initMisc();
}

Cluster::Cluster(Cluster::SubclusterTag, const Cluster & from, const std::vector<UInt64> & indices)
{
    for (auto index : indices)
    {
        shards_info.emplace_back(from.shards_info.at(index));

        if (!from.addresses_with_failover.empty())
            addresses_with_failover.emplace_back(from.addresses_with_failover.at(index));
    }

    initMisc();
}

const std::string & Cluster::ShardInfo::insertPathForInternalReplication(bool prefer_localhost_replica, bool use_compact_format) const
{
    if (!has_internal_replication)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "internal_replication is not set");

    const auto & paths = insert_path_for_internal_replication;
    if (!use_compact_format)
    {
        const auto & path = prefer_localhost_replica ? paths.prefer_localhost_replica : paths.no_prefer_localhost_replica;
        if (path.size() > NAME_MAX)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Path '{}' for async distributed INSERT is too long (exceed {} limit)", path, NAME_MAX);

        return path;
    }

    return paths.compact;
}

bool Cluster::maybeCrossReplication() const
{
    /// Cluster can be used for cross-replication if some replicas have different default database names,
    /// so one clickhouse-server instance can contain multiple replicas.

    if (addresses_with_failover.empty())
        return false;

    const String & database_name = addresses_with_failover.front().front().default_database;
    for (const auto & shard : addresses_with_failover)
        for (const auto & replica : shard)
            if (replica.default_database != database_name)
                return true;

    return false;
}

std::set<cluster::NodeID> Cluster::nodes() const
{
    std::set<cluster::NodeID> cluster_nodes;
    for (const auto & shard_info : shards_info)
    {
        for (const auto & addr : shard_info.all_addresses)
        {
            if (addr.node_id != cluster::Nulls::NullNodeID)
                cluster_nodes.insert(addr.node_id);
        }
    }

    return cluster_nodes;
}

}
