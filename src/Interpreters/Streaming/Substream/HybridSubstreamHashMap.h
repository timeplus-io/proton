#pragma once

#include <Interpreters/Streaming/Substream/ISubstreamHashMap.h>

#include <Checkpoint/RocksCheckpoint.h>
#include <Common/HashTable/Hash.h>
#include <Common/HybridHashTable/HybridHashTable.h>

namespace DB::Streaming
{
template <typename T>
class HybridSubstreamHashMap final : public ISubstreamHashMap
{
public:
    HybridSubstreamHashMap(
        const String & spill_dir,
        size_t max_hot_key_count,
        SubstreamValueSerializer && value_serializer_,
        SubstreamValueDeserializer && value_deserializer_,
        LoggerPtr logger)
    {
        HybridHashTableConfig config;
        config.spill_dir_path = spill_dir;
        config.max_hot_key_count = max_hot_key_count;

        config.value_object_size = sizeof(T);
        config.align_value_object_size = alignof(T);
        config.value_constructor = [](void * data) { new (data) T; };
        config.value_destructor = [](void * data) {
            auto * value = reinterpret_cast<T *>(data);
            value->~T();
        };
        config.value_serializer = std::move(value_serializer_);
        config.value_deserializer = std::move(value_deserializer_);

        auto key_serializer = [](const SubstreamID & id, DB::WriteBuffer & wb) {
            Streaming::serialize(id, wb);
            return DB::ErrorCodes::OK;
        };
        auto key_deserializer = [](SubstreamID & id, DB::ReadBuffer & rb) {
            Streaming::deserialize(id, rb);
            return DB::ErrorCodes::OK;
        };

        table = std::make_unique<HybridHashTable<Streaming::SubstreamID, UInt128TrivialHash>>(
            std::move(config), std::move(key_serializer), std::move(key_deserializer), logger);
    }

    HashTableType type() const noexcept override { return HashTableType::Hybrid; }

    std::pair<void *, bool> emplaceKey(const SubstreamID & id) override
    {
        auto emplace_result = table->emplaceKey(id, /*disable_spill=*/false);
        if (emplace_result.hasError())
            throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

        return {emplace_result.getMutableMapped(), emplace_result.isInserted()};
    }

    bool removeKey(const SubstreamID & id) override
    {
        auto err = table->removeKey(id);
        if (err) [[unlikely]]
            throw Exception(err, "remove key failed, err={}", ErrorCodes::getName(err));

        return true;
    }

    size_t size() const override { return table->approximateCount(); }

    void
    forEachKeyValue(const std::function<void(const SubstreamID & /*key*/, HybridMappedValue /*value*/)> & callback, bool inmemory) override
    {
        if (inmemory)
        {
            for (auto & [id, entry] : table->hotKeyValues())
                callback(id, entry);
        }
        else
        {
            table->forEachKeyValue(callback);
        }
    }

    std::string metricsString() const override
    {
        return fmt::format(
            "approx_substream_count={} total_buffer_bytes={} total_buffer_cells={} {}",
            table->approximateCount(),
            table->getBufferSizeInBytes(),
            table->getBufferSizeInCells(),
            table->getMetrics().string());
    }

    RocksCheckpointPtr createRocksCheckpoint(VersionType version)
    {
        table->flush();
        auto rocks_holder = table->getRocksHolder();
        chassert(rocks_holder);
        return std::make_shared<RocksCheckpoint>(version, std::move(rocks_holder));
    }

    void recoverFromRocksCheckpoint(RocksCheckpointPtr rocks_ckpt)
    {
        rocks_ckpt->recover(table->getConfig().spill_dir_path);
        table->reload();
    }

    void serialize(WriteBuffer & wb) const override { table->write(wb); }

    void deserialize(ReadBuffer & rb) override { table->read(rb); }

private:
    std::unique_ptr<HybridHashTable<Streaming::SubstreamID, UInt128TrivialHash>> table;
};

}
