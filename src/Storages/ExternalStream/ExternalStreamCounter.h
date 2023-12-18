#pragma once

#include <atomic>

namespace DB
{

class ExternalStreamCounter
{
public:
    inline uint64_t getReadBytes() const { return read_bytes.load(); }
    inline uint64_t getReadCounts() const { return read_counts.load(); }
    inline uint64_t getReadFailed() const { return read_failed.load(); }

    inline void addToReadBytes(uint64_t bytes) { read_bytes.fetch_add(bytes); }
    inline void addToReadCounts(uint64_t counts) { read_counts.fetch_add(counts); }
    inline void addToReadFailed(uint64_t amount) { read_failed.fetch_add(amount); }

    std::map<String, uint64_t> getCounters() const
    {
        return {
            {"ReadBytes", read_bytes.load()},
            {"ReadCounts", read_counts.load()},
            {"ReadFailed", read_failed.load()},
        };
    }

private:
    std::atomic<uint64_t> read_bytes;
    std::atomic<uint64_t> read_counts;
    std::atomic<uint64_t> read_failed;
};

using ExternalStreamCounterPtr = std::shared_ptr<ExternalStreamCounter>;
}
