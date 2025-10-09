#include <ClickHouse/ConnectionPool.h>

#include <boost/functional/hash.hpp>

namespace DB
{

namespace ClickHouse
{

ConnectionPoolPtr ConnectionPoolFactory::get(
    unsigned max_connections,
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
    Int64 priority)
{
    Key key{
        max_connections,
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
        priority,
        "relaxed",
        "",
        "",
        ""}; // Default SSL settings for backward compatibility

    std::lock_guard lock(mutex);
    auto [it, inserted] = pools.emplace(key, ConnectionPoolPtr{});
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    ConnectionPoolPtr ret{
        new ConnectionPool(
            max_connections,
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
            priority),
        [key, this](auto ptr) {
            {
                std::lock_guard another_lock(mutex);
                pools.erase(key);
            }
            delete ptr;
        }};
    it->second = ConnectionPoolWeakPtr(ret);
    return ret;
}

ConnectionPoolPtr ConnectionPoolFactory::get(
    unsigned max_connections,
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
    String ssl_key_file)
{
    Key key{
        max_connections,
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
        priority,
        ssl_verify_mode,
        ssl_ca_cert_file,
        ssl_cert_file,
        ssl_key_file};

    std::lock_guard lock(mutex);
    auto [it, inserted] = pools.emplace(key, ConnectionPoolPtr{});
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    ConnectionPoolPtr ret{
        new ConnectionPool(
            max_connections,
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
            priority,
            ssl_verify_mode,
            ssl_ca_cert_file,
            ssl_cert_file,
            ssl_key_file),
        [key, this](auto ptr) {
            {
                std::lock_guard another_lock(mutex);
                pools.erase(key);
            }
            delete ptr;
        }};
    it->second = ConnectionPoolWeakPtr(ret);
    return ret;
}

size_t ConnectionPoolFactory::KeyHash::operator()(const ConnectionPoolFactory::Key & k) const
{
    using boost::hash_combine;
    using boost::hash_value;
    size_t seed = 0;
    hash_combine(seed, hash_value(k.max_connections));
    hash_combine(seed, hash_value(k.host));
    hash_combine(seed, hash_value(k.port));
    hash_combine(seed, hash_value(k.default_database));
    hash_combine(seed, hash_value(k.user));
    hash_combine(seed, hash_value(k.password));
    hash_combine(seed, hash_value(k.cluster));
    hash_combine(seed, hash_value(k.cluster_secret));
    hash_combine(seed, hash_value(k.client_name));
    hash_combine(seed, hash_value(k.compression));
    hash_combine(seed, hash_value(k.secure));
    hash_combine(seed, hash_value(k.priority));
    hash_combine(seed, hash_value(k.ssl_verify_mode));
    hash_combine(seed, hash_value(k.ssl_ca_cert_file));
    hash_combine(seed, hash_value(k.ssl_cert_file));
    hash_combine(seed, hash_value(k.ssl_key_file));
    return seed;
}


ConnectionPoolFactory & ConnectionPoolFactory::instance()
{
    static ConnectionPoolFactory ret;
    return ret;
}

}

}
