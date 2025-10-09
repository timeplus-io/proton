#pragma once

#include <Core/HashTableType.h>
#include <Core/Streaming/SubstreamID.h>
#include <Common/HybridHashTable/HybridMappedValue.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;

namespace Streaming
{
using SubstreamValueSerializer = std::function<int(const void *, WriteBuffer &)>;
using SubstreamValueDeserializer = std::function<int(void *, ReadBuffer &)>;

class ISubstreamHashMap
{
public:
    virtual ~ISubstreamHashMap() = default;

    virtual HashTableType type() const noexcept = 0;

    virtual std::pair<void * /*value*/, bool /*inserted*/> emplaceKey(const SubstreamID & id) = 0;

    virtual bool removeKey(const SubstreamID & id) = 0;

    virtual size_t size() const = 0;

    virtual void forEachKeyValue(const std::function<void(const SubstreamID & /*key*/, HybridMappedValue /*value*/)> & callback, bool inmemory) = 0;

    virtual std::string metricsString() const = 0;

    virtual void serialize(WriteBuffer & wb) const = 0;
    virtual void deserialize(ReadBuffer & rb) = 0;
};

using SubstreamHashMapPtr = std::unique_ptr<ISubstreamHashMap>;
}
}
