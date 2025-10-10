#include <Cluster/Common/EncodeMetaKey.h>

#include <Cluster/Common/StreamIDShard.h>
#include <IO/PrefixTreeEncode.h>

#include <utility>

namespace cluster
{
MetaKeySpace decodeMetaKeySpace(std::string_view key)
{
    return static_cast<MetaKeySpace>(DB::PrefixTreeEncode::decodeVarUIntAscending(key));
}

std::string encodeMetaStreamKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Stream), key);
    return key;
}

std::string encodeMetaStreamKey(const std::string & ns)
{
    std::string key;
    key.reserve(1 + ns.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Stream), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    return key;
}

std::string encodeMetaStreamKey(const std::string & ns, const std::string & name)
{
    std::string key;
    key.reserve(1 + ns.size() + name.size() + 4);
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Stream), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaUDFKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::UDF), key);
    return key;
}

std::string encodeMetaUDFKey(const std::string & name)
{
    std::string key;
    key.reserve(1 + name.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::UDF), key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaFormatSchemaKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::FormatSchema), key);
    return key;
}

std::string encodeMetaFormatSchemaKey(const std::string & name, const std::string & format)
{
    std::string key;
    key.reserve(1 + name.size() + format.size() + 4);
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::FormatSchema), key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    DB::PrefixTreeEncode::encodeStringAscending(format, key);
    return key;
}

std::string encodeMetaAccessEntityKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::AccessEntity), key);
    return key;
}

std::string encodeMetaAccessEntityKey(const std::string & id)
{
    std::string key;
    key.reserve(1 + id.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::AccessEntity), key);
    DB::PrefixTreeEncode::encodeStringAscending(id, key);
    return key;
}

std::string encodeMetaNodeKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Node), key);
    return key;
}

std::string encodeMetaNodeKey(NodeID node_id)
{
    std::string key;
    key.reserve(4);

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Node), key);
    DB::PrefixTreeEncode::encodeVarUIntAscending(node_id, key);
    return key;
}

std::string encodeMetaDatabaseKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Database), key);
    return key;
}

std::string encodeMetaDatabaseKey(const std::string & name)
{
    std::string key;
    key.reserve(1 + name.size());

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Database), key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaDiskKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Disk), key);
    return key;
}

std::string encodeMetaDiskKey(const std::string & name)
{
    std::string key;
    key.reserve(1 + name.size());

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Disk), key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaStoragePolicyKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::StoragePolicy), key);
    return key;
}

std::string encodeMetaStoragePolicyKey(const std::string & name)
{
    std::string key;
    key.reserve(1 + name.size());

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::StoragePolicy), key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaAlertKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Alert), key);
    return key;
}

std::string encodeMetaAlertKey(const std::string & ns)
{
    std::string key;
    key.reserve(1 + ns.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Alert), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    return key;
}

std::string encodeMetaAlertKey(const std::string & ns, const std::string & name)
{
    std::string key;
    key.reserve(1 + ns.size() + name.size() + 4);
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Alert), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaMaterializedViewAssignmentKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::MaterializedViewAssignment), key);
    return key;
}

std::string encodeMetaMaterializedViewAssignmentKey(const std::string & ns, const std::string & name)
{
    std::string key;
    key.reserve(1 + ns.size() + name.size() + 4);
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::MaterializedViewAssignment), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaTaskKey()
{
    std::string key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Task), key);
    return key;
}

std::string encodeMetaTaskKey(const std::string & ns)
{
    std::string key;
    key.reserve(1 + ns.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Task), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    return key;
}

std::string encodeMetaTaskKey(const std::string & ns, const std::string & name)
{
    std::string key;
    key.reserve(1 + ns.size() + name.size() + 4);
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::Task), key);
    DB::PrefixTreeEncode::encodeStringAscending(ns, key);
    DB::PrefixTreeEncode::encodeStringAscending(name, key);
    return key;
}

std::string encodeMetaNamedCollectionKey()
{
    std::string encoded_key;
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::NamedCollection), encoded_key);
    return encoded_key;
}

std::string encodeMetaNamedCollectionKey(const std::string & name)
{
    std::string encoded_key;
    encoded_key.reserve(1 + name.size());
    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::NamedCollection), encoded_key);
    DB::PrefixTreeEncode::encodeStringAscending(name, encoded_key);
    return encoded_key;
}

std::string decodeMetaNamedCollectionKey(std::string_view data)
{
    DB::PrefixTreeEncode::decodeVarUIntAscending(data);
    std::string decoded;
    auto result = DB::PrefixTreeEncode::decodeStringAscending(data, decoded);
    return std::string{result.data(), result.size()};
}

namespace
{
std::string encodeMetaKey(MetaKeySpace key_space, const StreamIDShard & stream_shard)
{
    std::string key;
    key.reserve(1 + stream_shard.approximateSerializedSize() + 4);

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(key_space), key);
    DB::PrefixTreeEncode::encodeStringAscending(stream_shard.id.toUnderType().string_view(), key);
    DB::PrefixTreeEncode::encodeVarUIntAscending(stream_shard.shard, key);

    return key;
}
}

std::string encodeMetaAppliedSequenceKey(const StreamIDShard & stream_shard)
{
    return encodeMetaKey(MetaKeySpace::MetaAppliedSN, stream_shard);
}

std::string encodeMetaPendingRequestKey(uint64_t sequence_number)
{
    std::string key;
    key.reserve(1 + sizeof(uint64_t));  /// Reserve space for prefix + 8-byte uint64

    DB::PrefixTreeEncode::encodeVarUIntAscending(std::to_underlying(MetaKeySpace::PendingRequest), key);
    DB::PrefixTreeEncode::encodeUInt64Ascending(sequence_number, key);
    
    return key;
}
}
