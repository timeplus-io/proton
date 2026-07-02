#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SipHash.h>
#include <Common/serde.h>

namespace DB::Streaming
{
struct KeySet
{
    /// \param max_entries_ if it is == 0, no max entries limit
    /// \param timeout_sec_ if it is <= 0, no timeout limit
    KeySet(UInt32 max_entries_, Int32 timeout_sec_) : max_entries(max_entries_), timeout_interval_ms(timeout_sec_ * 1000) { }

    IColumn::Filter populateKeySetsAndCalculateResults(const ColumnsWithTypeAndName & arguments, size_t & rows);

    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

private:
    /// FIXME, if dedup column is integer, use them directly
    UInt32 max_entries;
    Int32 timeout_interval_ms;

    using Key = UInt128;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, UInt128Hash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

    mutable std::mutex mutex;

    Set key_set;

    struct KeyTime
    {
        explicit KeyTime(UInt128 key_) : key(key_), time(MonotonicMilliseconds::now()) { }
        explicit KeyTime(UInt128 key_, Int64 time_) : key(key_), time(time_) { }

        bool timedOut(Int32 timeout_) const noexcept { return MonotonicMilliseconds::now() - time > timeout_; }

        UInt128 key;
        Int64 time;
    };

    std::deque<KeyTime> keys;
};

/// DedupTransform dedup input column(s) according to the dedup keys
class DedupTransform final : public ISimpleTransform
{
public:
    DedupTransform(const Block & input_header, const Block & output_header, TableFunctionDescriptionPtr dedup_func_desc_);

    ~DedupTransform() override = default;

    String getName() const override { return "DedupTransform"; }

    void transform(Chunk & chunk) override;

    bool hasState() const override { return true; }
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

private:
    /// Calculate the positions of columns required by timestamp expr
    void init(const Block & input_header, const Block & output_header);
    void initKeySet();

private:
    ContextPtr context;

    TableFunctionDescriptionPtr dedup_func_desc;

    Chunk chunk_header;

    std::vector<size_t> expr_column_positions;
    std::vector<ssize_t> output_column_positions;

    SERDE std::unique_ptr<KeySet> key_set;
};

}
