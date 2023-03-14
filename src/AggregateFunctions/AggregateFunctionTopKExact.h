#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnArray.h>

#include <Common/assert_cast.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ArenaUtils.h>
#include <Common/SpaceSaving.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NOT_IMPLEMENTED;
}


template <typename TKey, typename Hash = DefaultHash<TKey>>
class SpaceSavingForTopKExact
{
public:
    using Self = SpaceSavingForTopKExact;

    struct Counter
    {
        Counter() = default; //-V730

        explicit Counter(const TKey & k, UInt64 c = 0, UInt64 e = 0, size_t h = 0) : key(k), slot(0), hash(h), count(c), error(e) { }

        void write(WriteBuffer & wb) const
        {
            writeBinary(key, wb);
            writeVarUInt(count, wb);
            writeVarUInt(error, wb);
        }

        void read(ReadBuffer & rb)
        {
            readBinary(key, rb);
            readVarUInt(count, rb);
            readVarUInt(error, rb);
        }

        // greater() taking slot error into account
        bool operator>(const Counter & b) const { return (count > b.count) || (count == b.count && error < b.error); }

        TKey key;
        size_t slot;
        size_t hash;
        UInt64 count;
        UInt64 error;
    };

    //default memory limit=100*1024*1024 byte=100M;
    explicit SpaceSavingForTopKExact(UInt64 memory_limit_bytes_ = 100 * 1024 * 1024):memory_limit_bytes(memory_limit_bytes_) { }

    ~SpaceSavingForTopKExact() { destroyElements(); }

    inline size_t size() const { return counter_list.size(); }

    void clear() { return destroyElements(); }

    void insert(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        checkAndPushCounter(key,increment,error);
    }

    void merge(const Self & rhs)
    {
        // The list is sorted in descending order, we have to scan in reverse
        for (auto * counter : boost::adaptors::reverse(rhs.counter_list))
        {
            checkAndPushCounter(counter->key, counter->count, counter->error);
        }
    }

    void checkAndPushCounter(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        size_t size = sizeof(TKey);
        if constexpr (std::is_same_v<TKey, StringRef>)
        {
            size += key.size;
        }

        auto hash = counter_map.hash(key);

        if (auto * counter = findCounter(key, hash); counter)
        {
            counter->count += increment;
            counter->error += error;
            percolate(counter);
            return;
        }

        if (unlikely(used_memory + size > memory_limit_bytes))
            throw Exception("top_k_exact reached maxium memory", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        used_memory += size;

        auto * c = new Counter(arena.emplace(key), increment, error, hash);
        push(c);

        return;
    }

    std::vector<Counter> topK(size_t k) const
    {
        std::vector<Counter> res;
        for (auto * counter : counter_list)
        {
            res.push_back(*counter);
            if (res.size() == k)
                break;
        }
        return res;
    }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(size(), wb);
        for (auto * counter : counter_list)
            counter->write(wb);
    }

    void read(ReadBuffer & rb)
    {
        destroyElements();
        size_t count = 0;
        readVarUInt(count, rb);

        for (size_t i = 0; i < count; ++i)
        {
            auto * counter = new Counter();
            counter->read(rb);
            counter->hash = counter_map.hash(counter->key);
            push(counter);
        }
    }

protected:
    void push(Counter * counter)
    {
        counter->slot = counter_list.size();
        counter_list.push_back(counter);
        counter_map[counter->key] = counter;
        percolate(counter);
    }

    // This is equivallent to one step of bubble sort
    void percolate(Counter * counter)
    {
        while (counter->slot > 0)
        {
            auto * next = counter_list[counter->slot - 1];
            if (*counter > *next)
            {
                std::swap(next->slot, counter->slot);
                std::swap(counter_list[next->slot], counter_list[counter->slot]);
            }
            else
                break;
        }
    }

private:
    void destroyElements()
    {
        for (auto * counter : counter_list)
        {
            arena.free(counter->key);
            delete counter;
        }

        counter_map.clear();
        counter_list.clear();
    }

    Counter * findCounter(const TKey & key, size_t hash)
    {
        auto it = counter_map.find(key, hash);
        if (!it)
            return nullptr;

        return it->getMapped();
    }

    void rebuildCounterMap()
    {
        counter_map.clear();
        for (auto * counter : counter_list)
            counter_map[counter->key] = counter;
    }

    using CounterMap = HashMapWithStackMemory<TKey, Counter *, Hash, 4>;

    CounterMap counter_map;
    std::vector<Counter *, AllocatorWithMemoryTracking<Counter *>> counter_list;
    SpaceSavingArena<TKey> arena;
    size_t memory_limit_bytes;
    size_t used_memory = 0;
};

template <typename T>
struct AggregateFunctionTopKExactData
{
    explicit AggregateFunctionTopKExactData(UInt64 memory_limit_bytes = 100 * 1024 * 1024):value(memory_limit_bytes){}   
    using Set = SpaceSavingForTopKExact<T, HashCRC32<T>>;

    Set value;
};
template <typename T, bool is_weighted>
class AggregateFunctionTopKExact
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKExactData<T>, AggregateFunctionTopKExact<T, is_weighted>>
{
protected:
    using State = AggregateFunctionTopKExactData<T>;
    UInt64 threshold;
    UInt64 memory_limit_bytes; //default memory size is 100MB;

public:
    AggregateFunctionTopKExact(UInt64 threshold_, UInt64 memory_limit_bytes_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKExactData<T>, AggregateFunctionTopKExact<T, is_weighted>>(argument_types_, params)
        , threshold(threshold_), memory_limit_bytes(memory_limit_bytes_) {}

    String getName() const override { return is_weighted ? "top_k_exact_weighted" : "top_k_exact"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(this->argument_types[0]);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) State(memory_limit_bytes);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & set = this->data(place).value;

        if constexpr (is_weighted)
            set.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], columns[1]->getUInt(row_num));
        else
            set.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.merge(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        auto result_vec = set.topK(threshold);
        size_t size = result_vec.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            data_to[old_size + i] = it->key;
    }
};

struct AggregateFunctionTopKExactGenericData
{
    using Set = SpaceSavingForTopKExact<StringRef, StringRefHash>;
    explicit AggregateFunctionTopKExactGenericData(UInt64 memory_limit_bytes = 100 * 1024 *1024):value(memory_limit_bytes){}
    Set value;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns top_k() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column, bool is_weighted>
class AggregateFunctionTopKExactGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKExactGenericData, AggregateFunctionTopKExactGeneric<is_plain_column, is_weighted>>
{
protected:
    using State = AggregateFunctionTopKExactGenericData;

    UInt64 threshold;
    UInt64 memory_limit_bytes; //default memory size is 100MB
    DataTypePtr & input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionTopKExactGeneric(
        UInt64 threshold_, UInt64 memory_limit_bytes_, const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionTopKExactGenericData, AggregateFunctionTopKExactGeneric<is_plain_column, is_weighted>>(argument_types_, params)
        , threshold(threshold_), memory_limit_bytes(memory_limit_bytes_), input_data_type(this->argument_types[0]) {}

    String getName() const override { return is_weighted ? "top_k_exact_weighted" : "top_k_exact"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(input_data_type);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        new (place) State(memory_limit_bytes);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        set.clear();

        // Specialized here because there's no deserialiser for StringRef
        size_t size = 0;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            UInt64 count;
            UInt64 error;
            readVarUInt(count, buf);
            readVarUInt(error, buf);
            set.insert(ref, count, error);
            arena->rollback(ref.size);
        }

    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if constexpr (is_plain_column)
        {
            if constexpr (is_weighted)
                set.insert(columns[0]->getDataAt(row_num), columns[1]->getUInt(row_num));
            else
                set.insert(columns[0]->getDataAt(row_num));
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            if constexpr (is_weighted)
                set.insert(str_serialized, columns[1]->getUInt(row_num));
            else
                set.insert(str_serialized);

            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.merge(this->data(rhs).value);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto result_vec = this->data(place).value.topK(threshold);
        offsets_to.push_back(offsets_to.back() + result_vec.size());

        for (auto & elem : result_vec)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.key.data, elem.key.size);
            else
                data_to.deserializeAndInsertFromArena(elem.key.data);
        }
    }
};

/// proton: starts. Extended with count

template <typename T, bool is_weighted>
class AggregateFunctionTopKExactWithCount : public AggregateFunctionTopKExact<T, is_weighted>
{
public:
    using AggregateFunctionTopKExact<T, is_weighted>::AggregateFunctionTopKExact;

    String getName() const override { return is_weighted ? "top_k_exact_weighted_with_count" : "top_k_exact_with_count"; }

    /// Result: (top_value, count)
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{this->argument_types[0], std::make_shared<DataTypeUInt64>()}));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & arr_to = assert_cast<ColumnArray &>(to);
        auto & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & set = this->data(place).value;
        const auto & result_vec = set.topK(this->threshold);

        auto & column_value = assert_cast<ColumnVector<T> &>(tuple_to.getColumn(0));
        auto & column_count = assert_cast<ColumnVector<UInt64> &>(tuple_to.getColumn(1));

        for (const auto & elem : result_vec)
        {
            column_value.getData().push_back(elem.key);
            column_count.getData().push_back(elem.count);
        }

        offsets_to.push_back(tuple_to.size());
    }
};

template <bool is_plain_column, bool is_weighted>
class AggregateFunctionTopKExactGenericWithCount : public AggregateFunctionTopKExactGeneric<is_plain_column, is_weighted>
{
public:
    using AggregateFunctionTopKExactGeneric<is_plain_column, is_weighted>::AggregateFunctionTopKExactGeneric;

    String getName() const override { return is_weighted ? "top_k_exact_weighted_with_count" : "top_k_exact_with_count"; }

    /// Result: (top_value, count)
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{this->input_data_type, std::make_shared<DataTypeUInt64>()}));
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & arr_to = assert_cast<ColumnArray &>(to);
        auto & tuple_to = assert_cast<ColumnTuple &>(arr_to.getData());
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & set = this->data(place).value;
        const auto & result_vec = set.topK(this->threshold);

        auto & column_value = tuple_to.getColumn(0);
        auto & column_count = assert_cast<ColumnVector<UInt64> &>(tuple_to.getColumn(1));

        for (const auto & elem : result_vec)
        {
            if constexpr (is_plain_column)
                column_value.insertData(elem.key.data, elem.key.size);
            else
                column_value.deserializeAndInsertFromArena(elem.key.data);

            column_count.getData().push_back(elem.count);
        }

        offsets_to.push_back(tuple_to.size());
    }
};
/// proton: ends.
}
