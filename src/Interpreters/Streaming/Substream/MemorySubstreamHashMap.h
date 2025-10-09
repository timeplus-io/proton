#pragma once

#include <Interpreters/Streaming/Substream/ISubstreamHashMap.h>

#include <absl/container/flat_hash_map.h>

namespace DB::Streaming
{
template <typename T>
class MemorySubstreamHashMap final : public ISubstreamHashMap
{
public:
    MemorySubstreamHashMap(SubstreamValueSerializer && value_serializer_, SubstreamValueDeserializer && value_deserializer_)
        : value_serializer(std::move(value_serializer_)), value_deserializer(std::move(value_deserializer_))
    {
    }

    HashTableType type() const noexcept override { return HashTableType::Memory; }

    std::pair<void *, bool> emplaceKey(const SubstreamID & id) override
    {
        auto [it, inserted] = map.try_emplace(id);
        return {&it->second, inserted};
    }

    bool removeKey(const SubstreamID & id) override { return map.erase(id); }

    size_t size() const override { return map.size(); }

    void forEachKeyValue(
        const std::function<void(const SubstreamID & /*key*/, HybridMappedValue /*value*/)> & callback, bool /*inmemory*/) override
    {
        HybridMappedValue::ControlBits control;
        HybridMappedValue mapped_value{nullptr, control};
        for (auto & [id, value] : map)
        {
            mapped_value.setData(&value);
            callback(id, mapped_value);
        }
    }

    std::string metricsString() const override
    {
        return fmt::format(
            "total_substream_count={} total_buffer_bytes={} total_buffer_cells={}",
            map.size(),
            (sizeof(map) + map.capacity() * sizeof(typename decltype(map)::value_type)),
            map.capacity());
    }

    void serialize(WriteBuffer & wb) const override
    {
        writeIntBinary(map.size(), wb);
        for (const auto & [id, value] : map)
        {
            Streaming::serialize(id, wb);
            value_serializer(&value, wb);
        }
    }

    void deserialize(ReadBuffer & rb) override
    {
        size_t num_substreams = 0;
        readIntBinary(num_substreams, rb);
        map.reserve(num_substreams);
        for (size_t i = 0; i < num_substreams; ++i)
        {
            SubstreamID id;
            Streaming::deserialize(id, rb);

            auto & value = map[id];
            value_deserializer(&value, rb);
        }
    }

private:
    absl::flat_hash_map<Streaming::SubstreamID, T, UInt128TrivialHash> map;
    SubstreamValueSerializer value_serializer;
    SubstreamValueDeserializer value_deserializer;
};

}
