#pragma once

#include <Client/ConnectionPool.h>
#include <Client/ConnectionPoolWithFailover.h>

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>

#include <Poco/Net/SocketAddress.h>

#include <string>

namespace DB
{

struct Settings;

/// Cluster contains connection pools to each node
/// With the local nodes, the connection is not established, but the request is executed directly.
/// Therefore we store only the number of local nodes
/// In the config, the cluster includes nodes <node> or <shard>
class Cluster
{
public:
    /// Construct a cluster by the names of shards and replicas.
    /// Local are treated as well as remote ones if treat_local_as_remote is true.
    /// Local are also treated as remote if treat_local_port_as_remote is set and the local address includes a port
    /// 'default_tcp_port' - default tcp :port that a server instance listen for queries.
    /// This parameter is needed only to check that some address is local (points to ourself).
    ///
    /// Used for remote() function.
    Cluster(
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
        bool secure = false,
        Int64 priority = 1);

    Cluster(const Cluster &) = delete;
    Cluster & operator=(const Cluster &) = delete;

    /// is used to set a limit on the size of the timeout
    static Poco::Timespan saturate(Poco::Timespan v, Poco::Timespan limit);

public:
    using SlotToShard = std::vector<UInt64>;

    struct Address
    {
        String host_name;
        cluster::NodeID node_id = cluster::Nulls::NullNodeID;
        UInt16 port{0};
        String user;
        String password;
        String quota_key;

        /// For inter-server authorization
        String cluster;
        String cluster_secret;

        UInt32 shard_index{}; /// shard serial number in configuration file, starting from 1.
        UInt32 replica_index{}; /// replica serial number in this shard, starting from 1; zero means no replicas.

        /// This database is selected when no database is specified for Distributed table
        String default_database;
        /// The locality is determined at the initialization, and is not changed even if DNS is changed
        bool is_local = false;

        Protocol::Compression compression = Protocol::Compression::Enable;
        Protocol::Secure secure = Protocol::Secure::Disable;

        Int64 priority = 1;

        Address() = default;

        Address(
            const String & host_port_,
            cluster::NodeID node_id_,
            const String & user_,
            const String & password_,
            const String & cluster_id_,
            const String & cluster_secret_,
            UInt16 default_tcp_port,
            bool treat_local_port_as_remote,
            bool is_local_,
            bool secure_ = false,
            Int64 priority_ = 1,
            UInt32 shard_index_ = 0,
            UInt32 replica_index_ = 0);

        /// Returns 'escaped_host_name:port'
        String toString() const;

        /// Returns 'host_name:port'
        String readableString() const;

        static String toString(const String & host_name, UInt16 port);

        static std::pair<String, UInt16> fromString(const String & host_port_string);

        /// Returns escaped shard{shard_index}_replica{replica_index} or escaped
        /// user:password@resolved_host_address:resolved_host_port#default_database
        /// depending on use_compact_format flag
        String toFullString(bool use_compact_format) const;

        /// Returns address with only shard index and replica index or full address without shard index and replica index
        static Address fromFullString(const String & address_full_string);

        bool operator==(const Address & other) const noexcept
        {
            return host_name == other.host_name && port == other.port && secure == other.secure && user == other.user
                && password == other.password && cluster == other.cluster && cluster_secret == other.cluster_secret
                && default_database == other.default_database;
        }
    };

    using Addresses = std::vector<Address>;
    using AddressesWithFailover = std::vector<Addresses>;
    /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
    ///
    /// Contains different path for permutations of:
    /// - prefer_localhost_replica
    ///   Notes with prefer_localhost_replica==0 will contains local nodes.
    /// - use_compact_format_in_distributed_parts_names
    ///   See toFullString()
    ///
    /// This is cached to avoid looping by replicas in insertPathForInternalReplication().
    struct ShardInfoInsertPathForInternalReplication
    {
        /// prefer_localhost_replica == 1 && use_compact_format_in_distributed_parts_names=0
        std::string prefer_localhost_replica;
        /// prefer_localhost_replica == 0 && use_compact_format_in_distributed_parts_names=0
        std::string no_prefer_localhost_replica;
        /// use_compact_format_in_distributed_parts_names=1
        std::string compact;
    };

    struct ShardInfo
    {
    public:
        bool isLocal() const { return !local_addresses.empty(); }
        bool hasRemoteConnections() const { return local_addresses.size() != per_replica_pools.size(); }
        bool hasLocalConnections() const { return !local_addresses.empty(); }
        size_t getLocalNodeCount() const { return local_addresses.size(); }
        size_t getRemoteNodeCount() const { return per_replica_pools.size() - local_addresses.size(); }
        size_t getAllNodeCount() const { return per_replica_pools.size(); }
        bool hasInternalReplication() const { return has_internal_replication; }
        /// Name of directory for asynchronous write to StorageDistributed if has_internal_replication
        const std::string & insertPathForInternalReplication(bool prefer_localhost_replica, bool use_compact_format) const;

    public:
        ShardInfoInsertPathForInternalReplication insert_path_for_internal_replication;

        /// Number of the shard, the indexation begins with 1
        UInt32 shard_num = 0;
        UInt32 weight = 1;
        Addresses local_addresses;
        Addresses all_addresses;
        /// nullptr if there are no remote addresses
        ConnectionPoolWithFailoverPtr pool;
        /// Connection pool for each replica, contains nullptr for local replicas
        ConnectionPoolPtrs per_replica_pools;
        bool has_internal_replication = false;
    };

    using ShardsInfo = std::vector<ShardInfo>;

    const ShardsInfo & getShardsInfo() const noexcept { return shards_info; }
    const ShardInfo & getAnyShardInfo() const;
    const AddressesWithFailover & getShardsAddresses() const noexcept { return addresses_with_failover; }

    /// The number of remote shards.
    size_t getRemoteShardCount() const noexcept { return remote_shard_count; }

    /// The number of clickhouse nodes located locally
    /// we access the local nodes directly.
    size_t getLocalShardCount() const noexcept { return local_shard_count; }

    /// The number of all shards.
    size_t getShardCount() const noexcept { return shards_info.size(); }

    const String & getSecret() const noexcept { return secret; }

    /// Get a subcluster consisting of one shard - index by count (from 0) of the shard of this cluster.
    std::unique_ptr<Cluster> getClusterWithSingleShard(size_t index) const;

    /// Get a subcluster consisting of one or multiple shards - indexes by count (from 0) of the shard of this cluster.
    std::unique_ptr<Cluster> getClusterWithMultipleShards(const std::vector<UInt64> & indices) const;

    /// Get a new Cluster that contains all servers (all shards with all replicas) from existing cluster as independent shards.
    std::unique_ptr<Cluster> getClusterWithReplicasAsShards(const Settings & settings, size_t max_replicas_from_shard = 0) const;

    /// Returns false if cluster configuration doesn't allow to use it for cross-replication.
    /// NOTE: true does not mean, that it's actually a cross-replication cluster.
    bool maybeCrossReplication() const;

    const String & getName() const { return name; }

    std::set<cluster::NodeID> nodes() const;

private:
    SlotToShard slot_to_shard;

public:
    const SlotToShard & getSlotToShard() const { return slot_to_shard; }

private:
    void initMisc();

    /// For getClusterWithMultipleShards implementation.
    struct SubclusterTag
    {
    };
    Cluster(SubclusterTag, const Cluster & from, const std::vector<UInt64> & indices);

    /// For getClusterWithReplicasAsShards implementation
    struct ReplicasAsShardsTag
    {
    };
    Cluster(ReplicasAsShardsTag, const Cluster & from, const Settings & settings, size_t max_replicas_from_shard);

    /// Inter-server secret
    String secret;

    /// Description of the cluster shards.
    ShardsInfo shards_info;

    /// Non-empty is either addresses or addresses_with_failover.
    /// The size and order of the elements in the corresponding array corresponds to shards_info.

    /// An array of shards. For each shard, an array of replica addresses (servers that are considered identical).
    AddressesWithFailover addresses_with_failover;

    size_t remote_shard_count = 0;
    size_t local_shard_count = 0;

    String name;
};

using ClusterPtr = std::shared_ptr<Cluster>;

}
