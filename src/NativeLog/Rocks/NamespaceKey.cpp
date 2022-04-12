#include "NamespaceKey.h"

#include <fmt/format.h>

namespace nlog
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
/// For example `/ns1/stream1`
std::string namespaceKey(const std::string & ns, const std::string & key, char ns_sep)
{
    assert(!ns.empty());

    return fmt::format("{0}{1}{0}{2}", ns_sep, ns, key);
}
}
