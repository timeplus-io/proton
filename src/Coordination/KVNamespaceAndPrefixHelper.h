#pragma once

#include "KVRequest.h"
#include "KVResponse.h"

#include <Common/SipHash.h>

#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>

namespace DB::ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int BAD_REQUEST_PARAMETER;
}

namespace Coordination
{
namespace
{
    const char NAMESPACE_SEPARATOR = '/';
}

class NamespacePrefixTransform : public rocksdb::SliceTransform
{
private:
    char separator;
    std::string name_;

public:
    explicit NamespacePrefixTransform(char separator_ = NAMESPACE_SEPARATOR)
        : separator(separator_), name_(fmt::format("Proton.rocksdb.NamespacePrefix'{}'", separator_))
    {
    }

    const char * Name() const override { return name_.c_str(); }

    /// prefix => top-namespace
    rocksdb::Slice Transform(const rocksdb::Slice & src) const override
    {
        auto begin = src.data();
        auto end = begin + src.size();
        auto pos = std::find(begin, end, separator);
        if (pos == end)
            return {};
        return rocksdb::Slice(begin, pos - begin);
    }

    bool InDomain(const rocksdb::Slice & src) const override
    {
        auto begin = src.data();
        auto end = begin + src.size();
        auto pos = std::find(begin, end, separator);
        if (pos == end)
            return false;
        return true;
    }

    bool SameResultWhenAppended(const rocksdb::Slice &) const override { return false; }
};


//// Handle namespace and prefix, main purposes:
/// 1) Before request, set column family and set prefix if necessary
/// 2) After request, filter sub-prefix for response key if necessary
///
/// For namespace handling, there are some cases in PUT/GET request:
///     namespace   +   [prefix]@key - @value
/// (1) "proton"    +   @key        - PUT: store @key-@value into 'proton' column family.
///     {column_family}             - GET: find @key from 'proton' column family and return @key-@value if is exists
///
/// (2) "proton"    +   @table/key  - PUT: store 'table'/@key - @value into 'proton' column family.
///     {column_family} {prefix}    - GET: find 'table'/@key from 'proton' column family by prefix seek and return @table/key-@value if is exists
///
/// (3) "proton"    +   @user/table/key  -- for this case, we split {prefix} -> {top-prefix}/{sub-prefix...}, only use {top-prefix} to prefix seek in metastore
///     {column_family} {top-prefix}/{sub-prefix...}/key
///                                 - PUT: store 'user/table/key' into 'proton' column family.
///                                 - GET: find 'user/*' from 'proton' column family by prefix seek
///                                        and filter 'user/*' by 'user/table' again, then return @user/table/key-@value if is exists
/// In other words:
/// 1) column family (search by partition)
/// 2) top-prefix (search by index)
/// 3) sub-prefix (normal filter)
class KVNamespaceAndPrefixHelper final
{
private:
    const String & column_family;
    String prefix;

public:
    explicit KVNamespaceAndPrefixHelper(const String & namespace_, const String & prefix_ = {})
        : column_family(namespace_), prefix(prefix_)
    {
        if (!prefix.empty())
            initPrefix(prefix_);
    }

    inline const String & getColumnFamily() const { return column_family; }

    inline const String & getPrefix() const { return prefix; }

    /// Add column_family [and prefix]
    KVRequest & handle(KVRequest & request)
    {
        request.column_family = column_family;
        if (request.getOpNum() == Coordination::KVOpNum::LIST)
        {
            auto & key_prefix = request.as<Coordination::KVListRequest>()->key_prefix;
            initPrefix(key_prefix);
            key_prefix = prefix;
        }

        return request;
    }

    /// Further filtering query results manually by nested namespaces if there are nested namespaces since
    /// RocksDB only supports one level prefix / namespace filtering.
    KVResponse & handle(KVResponse & response)
    {
        if (response.getOpNum() == Coordination::KVOpNum::LIST)
        {
            auto resp = response.as<Coordination::KVListResponse>();
            /// Filter by sub-prefix
            /// FIXME: better implementation?
            if (hasMultipleLevelPrefix())
                filter(resp->kv_pairs);
        }
        return response;
    }

    KVRequestPtr handle(KVRequestPtr request)
    {
        handle(*request);
        return request;
    }

    KVResponsePtr handle(KVResponsePtr response)
    {
        handle(*response);
        return response;
    }

private:
    inline bool hasMultipleLevelPrefix() const
    {
        auto pos = prefix.find_first_of(NAMESPACE_SEPARATOR);
        /// One level: 'a', 'a/'
        /// Multi level: 'a/b', 'a/b/'
        return (pos != std::string::npos && pos != prefix.size() - 1);
    }

    inline KVStringPairs & filter(KVStringPairs & pairs) const
    {
        for (auto iter = pairs.begin(); iter != pairs.end();)
        {
            if (iter->first.starts_with(prefix))
                ++iter;
            else
                iter = pairs.erase(iter);
        }
        return pairs;
    }

    void initPrefix(const String & prefix_)
    {
        prefix = prefix_;
        /// prefix: 'a' -> 'a/'
        if (!prefix_.empty() && prefix.find(NAMESPACE_SEPARATOR) == std::string::npos)
            prefix += NAMESPACE_SEPARATOR;
    }
};

using KVNamespaceAndPrefixHelperPtr = std::unique_ptr<KVNamespaceAndPrefixHelper>;
}
