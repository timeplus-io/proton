#pragma once

#include <ClickHouse/Connection.h>
#include <Core/Settings.h>
#include <IO/ConnectionTimeouts.h>
#include <base/defines.h>
#include <Common/PoolBase.h>

namespace DB
{

namespace ClickHouse
{

/** Interface for connection pools.
  *
  * Usage (using the usual `ConnectionPool` example)
  * ConnectionPool pool(...);
  *
  *    void thread()
  *    {
  *        auto connection = pool.get();
  *        connection->sendQuery(...);
  *    }
  */

class IConnectionPool : private boost::noncopyable
{
public:
    using Entry = PoolBase<Connection>::Entry;

    virtual ~IConnectionPool() = default;

    /// Selects the connection to work.
    /// If force_connected is false, the client must manually ensure that returned connection is good.
    virtual std::optional<Entry> get(const ConnectionTimeouts & timeouts, const Settings * settings, bool force_connected) = 0;

    virtual Int64 getPriority() const { return 1; }
};

using ConnectionPoolPtr = std::shared_ptr<IConnectionPool>;
using ConnectionPoolPtrs = std::vector<ConnectionPoolPtr>;

/** A common connection pool, without fault tolerance.
  */
class ConnectionPool : public IConnectionPool, private PoolBase<Connection>
{
public:
    using Entry = IConnectionPool::Entry;
    using Base = PoolBase<Connection>;

    ConnectionPool(
        unsigned max_connections_,
        const String & host_,
        UInt16 port_,
        const String & default_database_,
        const String & user_,
        const String & password_,
        const String & quota_key_,
        const String & cluster_,
        const String & cluster_secret_,
        const String & client_name_,
        Protocol::Compression compression_,
        Protocol::Secure secure_,
        Int64 priority_ = 1,
        const String & ssl_verify_mode_ = "relaxed",
        const String & ssl_ca_cert_file_ = "",
        const String & ssl_cert_file_ = "",
        const String & ssl_key_file_ = "")
        : Base(max_connections_, getLogger("ConnectionPool (" + host_ + ":" + toString(port_) + ")"))
        , host(host_)
        , port(port_)
        , default_database(default_database_)
        , user(user_)
        , password(password_)
        , quota_key(quota_key_)
        , cluster(cluster_)
        , cluster_secret(cluster_secret_)
        , client_name(client_name_)
        , compression(compression_)
        , secure(secure_)
        , priority(priority_)
        , ssl_verify_mode(ssl_verify_mode_)
        , ssl_ca_cert_file(ssl_ca_cert_file_)
        , ssl_cert_file(ssl_cert_file_)
        , ssl_key_file(ssl_key_file_)
    {
    }

    std::optional<Entry> get(const ConnectionTimeouts & timeouts, const Settings * settings, bool force_connected) override
    {
        std::optional<Entry> entry;
        if (settings != nullptr)
        {
            entry = Base::get(settings->connection_pool_max_wait_ms.totalMilliseconds(), /*return_on_timeout=*/true);
            if (entry->isNull())
                return std::nullopt;
        }
        else
        {
            entry = Base::get(-1);
        }

        if (force_connected)
            entry.value()->forceConnected(timeouts);

        return entry;
    }

    const std::string & getHost() const { return host; }
    std::string getDescription() const { return host + ":" + toString(port); }

    Int64 getPriority() const override { return priority; }

protected:
    /** Creates a new object to put in the pool. */
    ConnectionPtr allocObject() override
    {
        return std::make_shared<Connection>(
            host,
            port,
            default_database,
            user,
            password,
            quota_key,
            cluster,
            cluster_secret,
            client_name,
            compression,
            secure,
            Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0),
            ssl_verify_mode,
            ssl_ca_cert_file,
            ssl_cert_file,
            ssl_key_file);
    }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;
    String quota_key;

    /// For inter-server authorization
    String cluster;
    String cluster_secret;

    String client_name;
    Protocol::Compression compression; /// Whether to compress data when interacting with the server.
    Protocol::Secure secure; /// Whether to encrypt data when interacting with the server.
    Int64 priority; /// priority from <remote_servers>
    String ssl_verify_mode; /// SSL certificate verification mode
    String ssl_ca_cert_file; /// Path to SSL CA certificate file
    String ssl_cert_file; /// Path to SSL certificate file
    String ssl_key_file; /// Path to SSL private key file
};

/**
 * Connection pool factory. Responsible for creating new connection pools and reuse existing ones.
 */
class ConnectionPoolFactory final : private boost::noncopyable
{
public:
    struct Key
    {
        unsigned max_connections;
        String host;
        UInt16 port;
        String default_database;
        String user;
        String password;
        String quota_key;
        String cluster;
        String cluster_secret;
        String client_name;
        Protocol::Compression compression;
        Protocol::Secure secure;
        Int64 priority;
        String ssl_verify_mode;
        String ssl_ca_cert_file;
        String ssl_cert_file;
        String ssl_key_file;
    };

    struct KeyHash
    {
        size_t operator()(const ConnectionPoolFactory::Key & k) const;
    };

    static ConnectionPoolFactory & instance();

    ConnectionPoolPtr
    get(unsigned max_connections,
        String host,
        UInt16 port,
        String default_database,
        String user,
        String password,
        String quota_key,
        String cluster,
        String cluster_secret,
        String client_name,
        Protocol::Compression compression,
        Protocol::Secure secure,
        Int64 priority);

    ConnectionPoolPtr
    get(unsigned max_connections,
        String host,
        UInt16 port,
        String default_database,
        String user,
        String password,
        String quota_key,
        String cluster,
        String cluster_secret,
        String client_name,
        Protocol::Compression compression,
        Protocol::Secure secure,
        Int64 priority,
        String ssl_verify_mode,
        String ssl_ca_cert_file,
        String ssl_cert_file,
        String ssl_key_file);

private:
    mutable std::mutex mutex;
    using ConnectionPoolWeakPtr = std::weak_ptr<IConnectionPool>;
    std::unordered_map<Key, ConnectionPoolWeakPtr, KeyHash> pools TSA_GUARDED_BY(mutex);
};

inline bool operator==(const ConnectionPoolFactory::Key & lhs, const ConnectionPoolFactory::Key & rhs)
{
    return lhs.max_connections == rhs.max_connections && lhs.host == rhs.host && lhs.port == rhs.port
        && lhs.default_database == rhs.default_database && lhs.user == rhs.user && lhs.password == rhs.password
        && lhs.cluster == rhs.cluster && lhs.cluster_secret == rhs.cluster_secret && lhs.client_name == rhs.client_name
        && lhs.compression == rhs.compression && lhs.secure == rhs.secure && lhs.priority == rhs.priority
        && lhs.ssl_verify_mode == rhs.ssl_verify_mode && lhs.ssl_ca_cert_file == rhs.ssl_ca_cert_file
        && lhs.ssl_cert_file == rhs.ssl_cert_file && lhs.ssl_key_file == rhs.ssl_key_file;
}

}

}
