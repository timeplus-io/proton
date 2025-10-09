#include <Core/Field.h>
#include <Core/Joins.h>
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/HashJoin/RangeAsofJoinContext.h>
#include <Interpreters/Streaming/SortedLookupContainer.h>

#include <list>

namespace DB::Streaming
{

/// For All JOINs (non-unique JOINs)
/// RowBatch is per key
/// A key can have multiple rows
template <typename DataBlock>
struct RowBatch
{
    static constexpr size_t block_size = DEFAULT_BLOCK_SIZE;

    void insert(Block & block, size_t row)
    {
        if (!batches.empty() && batches.back().rows() < block_size)
        {
            batches.back().insert(block, row);
        }
        else
        {
            batches.emplace_back();
            batches.back().insert(block, row);
        }
    }

    void serialize(WriteBuffer & wb, const Block & header) const
    {
        writeVarUInt(batches.size(), wb);

        SimpleNativeWriter<DataBlock> writer{wb, header, /*version=*/1};
        for (const auto & data_block : batches)
            writer.write(data_block);
    }

    void deserialize(ReadBuffer & rb, const Block & header)
    {
        uint64_t batch_size = 0;
        readVarUInt(batch_size, rb);

        batches.reserve(batch_size);

        SimpleNativeReader<DataBlock> reader(rb, header, /*version=*/1);
        for (uint64_t n = 0; n < batch_size; ++n)
            batches.push_back(reader.read());
    }

    std::vector<DataBlock> batches;
};

struct MutableRows
{
    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

    void insert(Block & block, size_t row);
    bool empty() const noexcept { return rows.empty(); }

    std::vector<Row> rows;
};

struct RowListRef
{
    std::optional<std::list<Row>::iterator> iter;
};

struct RowList
{
    auto lastRowIter()
    {
        chassert(!rows.empty());
        return --rows.end();
    }

    void remove(const RowListRef & ref)
    {
        chassert(ref.iter);
        rows.erase(ref.iter.value());
    }

    bool empty() const noexcept { return rows.empty(); }

    void insert(Block & block, size_t row);
    void serialize(WriteBuffer & wb) const;
    void deserialize(ReadBuffer & rb);

    std::list<Row> rows;
};

struct AsofRows
{
    template <typename T>
    struct Entry
    {
        using LookupType = SortedLookupContainer<RowListRef, Entry<T>>;
        using LookupPtr = std::unique_ptr<LookupType>;

        Entry() { }
        explicit Entry(T v) : asof_value(v) { }
        Entry(T v, RowListRef row_ref_) : asof_value(v), row_ref(row_ref_) { }

        T asof_value;
        RowListRef row_ref;
    };

    using Lookups = std::variant<
        typename Entry<UInt8>::LookupPtr,
        typename Entry<UInt16>::LookupPtr,
        typename Entry<UInt32>::LookupPtr,
        typename Entry<UInt64>::LookupPtr,
        typename Entry<Int8>::LookupPtr,
        typename Entry<Int16>::LookupPtr,
        typename Entry<Int32>::LookupPtr,
        typename Entry<Int64>::LookupPtr,
        typename Entry<Float32>::LookupPtr,
        typename Entry<Float64>::LookupPtr,
        typename Entry<Decimal32>::LookupPtr,
        typename Entry<Decimal64>::LookupPtr,
        typename Entry<Decimal128>::LookupPtr,
        typename Entry<DateTime64>::LookupPtr>;

    AsofRows(TypeIndex t);

    void
    insert(TypeIndex type, const IColumn & asof_column, Block & block, size_t row_num, ASOFJoinInequality inequality, size_t keep_versions);

    /// This will be synchronized by the rwlock mutex in StreamingHashJoin.h
    const RowListRef * findAsof(TypeIndex type, ASOFJoinInequality inequality, const IColumn & asof_column, size_t row_num) const;

    void serialize(WriteBuffer & wb, TypeIndex type) const;
    void deserialize(ReadBuffer & wb, TypeIndex type);

private:
    RowList rows;

    /// Lookups can be stored in a HashTable because it is memmovable
    /// A std::variant contains a currently active type id (memmovable), together with a union of the types
    /// The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    /// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

class RangeAsofRows
{
public:
    /// FIXME, multimap data structure
    template <typename KeyT>
    using LookupType = std::multimap<KeyT, RowListRef>;

    template <typename KeyT>
    using LookupPtr = std::unique_ptr<LookupType<KeyT>>;

    using Lookups = std::variant<
        LookupPtr<UInt8>,
        LookupPtr<UInt16>,
        LookupPtr<UInt32>,
        LookupPtr<UInt64>,
        LookupPtr<Int8>,
        LookupPtr<Int16>,
        LookupPtr<Int32>,
        LookupPtr<Int64>,
        LookupPtr<Float32>,
        LookupPtr<Float64>,
        LookupPtr<Decimal32>,
        LookupPtr<Decimal64>,
        LookupPtr<Decimal128>,
        LookupPtr<DateTime64>>;

    explicit RangeAsofRows(TypeIndex t);

    void insert(TypeIndex type, const IColumn & asof_column, Block & block, size_t row_num);

    /// Find a range of rows which can be joined
    std::vector<RowListRef> findRange(
        TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num, bool is_left_block) const;

    /// Find the last one
    const RowListRef *
    findAsof(TypeIndex type, const RangeAsofJoinContext & range_join_ctx, const IColumn & asof_column, size_t row_num) const;

    void serialize(WriteBuffer & wb, TypeIndex type) const;
    void deserialize(ReadBuffer & wb, TypeIndex type);

private:
    RowList rows;

    /// Lookups can be stored in a HashTable because it is memmovable
    /// A std::variant contains a currently active type id (memmovable), together with a union of the types
    /// The types are all std::unique_ptr, which contains a single pointer, which is memmovable.
    /// Source: https://github.com/ClickHouse/ClickHouse/issues/4906
    Lookups lookups;
};

}
