#include <Cluster/Rocks/NamespaceKey.h>

#include <fmt/format.h>

namespace cluster
{
rocksdb::Slice NamespaceTransform::doTransform(const rocksdb::Slice & s) const
{
    if (s.empty())
        return s;

    auto start = s.data();
    auto end = start + s.size();

    /// Not prefixed with ns_sep
    if (start[0] != ns_sep)
        return {};

    /// Find namespace
    auto pos = std::find(start + 1, end, ns_sep);
    if (pos == end)
        return {};

    return rocksdb::Slice{start, pos - start + 1ul};
}

/// Concatenate namespaces and key
/// For example `/ns1`, `/ns1/stream1`
std::string namespaceKey(const std::string & ns, char ns_sep)
{
    assert(!ns.empty());
    return fmt::format("{}{}", ns_sep, ns);
}

std::string namespaceKey(const std::string & ns, std::string_view key, char ns_sep)
{
    assert(!ns.empty() && !key.empty());
    return fmt::format("{0}{1}{0}{2}", ns_sep, ns, key);
}

std::string namespaceKey(const std::string & ns, std::string_view ns2, std::string_view key, char ns_sep)
{
    assert(!ns.empty() && !ns2.empty() && !key.empty());
    return fmt::format("{0}{1}{0}{2}{0}{3}", ns_sep, ns, ns2, key);
}

/// `/ns1/`
std::string namespaceKeyPrefix(const std::string & ns, char ns_sep)
{
    assert(!ns.empty());
    return fmt::format("{0}{1}{0}", ns_sep, ns);
}
}
