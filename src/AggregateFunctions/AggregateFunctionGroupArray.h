#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

enum class Sampler
{
    NONE,
    RNG,
    DETERMINATOR // TODO
};

template <bool Thas_limit, bool Tlast, Sampler Tsampler>
struct GroupArrayTrait
{
    static constexpr bool has_limit = Thas_limit;
    static constexpr bool last = Tlast;
    static constexpr Sampler sampler = Tsampler;
};

template <typename Trait>
static constexpr const char * getNameByTrait()
{
    if (Trait::last)
        return "group_array_last";
    if (Trait::sampler == Sampler::NONE)
        return "group_array";
    else if (Trait::sampler == Sampler::RNG)
        return "group_array_sample";
    // else if (Trait::sampler == Sampler::DETERMINATOR) // TODO

    UNREACHABLE();
}

template <typename T>
struct GroupArraySamplerData
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
    size_t total_values = 0;
    pcg32_fast rng;

    UInt64 genRandom(size_t lim)
    {
        /// With a large number of values, we will generate random numbers several times slower.
        if (lim <= static_cast<UInt64>(rng.max()))
            return static_cast<UInt32>(rng()) % static_cast<UInt32>(lim);
        else
            return (static_cast<UInt64>(rng()) * (static_cast<UInt64>(rng.max()) + 1ULL) + static_cast<UInt64>(rng())) % lim;
    }

    void randomShuffle()
    {
        for (size_t i = 1; i < value.size(); ++i)
        {
            size_t j = genRandom(i + 1);
            std::swap(value[i], value[j]);
        }
    }
};

/// A particular case is an implementation for numeric types.
template <typename T, bool has_sampler>
struct GroupArrayNumericData;

template <typename T>
struct GroupArrayNumericData<T, false>
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    // For group_array_last()
    size_t total_values = 0;
    Array value;
};

template <typename T>
struct GroupArrayNumericData<T, true> : public GroupArraySamplerData<T>
{
};

template <typename T, typename Trait>
class GroupArrayNumericImpl final
    : public IAggregateFunctionDataHelper<GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>, GroupArrayNumericImpl<T, Trait>>
{
    using Data = GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>;
    static constexpr bool limit_num_elems = Trait::has_limit;
    UInt64 max_elems;
    UInt64 seed;

public:
    explicit GroupArrayNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>, GroupArrayNumericImpl<T, Trait>>(
            {data_type_}, parameters_)
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return getNameByTrait<Trait>(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(this->argument_types[0]); }

    void insertWithSampler(Data & a, const T & v, Arena * arena) const
    {
        ++a.total_values;
        if (a.value.size() < max_elems)
        {
            a.value.push_back(v, arena);
        }
        else
        {
            UInt64 rnd = a.genRandom(a.total_values);
            if (rnd < max_elems)
                a.value[rnd] = v;
        }
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
        if constexpr (Trait::sampler == Sampler::RNG)
            a->rng.seed(seed);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        auto & cur_elems = this->data(place);

        ++cur_elems.total_values;

        if constexpr (Trait::sampler == Sampler::NONE)
        {
            if (limit_num_elems && cur_elems.value.size() >= max_elems)
            {
                if constexpr (Trait::last)
                    cur_elems.value[(cur_elems.total_values - 1) % max_elems] = row_value;

                return;
            }

            cur_elems.value.push_back(row_value, arena);
        }

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            if (cur_elems.value.size() < max_elems)
                cur_elems.value.push_back(row_value, arena);
            else
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < max_elems)
                    cur_elems.value[rnd] = row_value;
            }
        }
        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        if (rhs_elems.value.empty())
            return;

        if constexpr (Trait::last)
            mergeNoSamplerLast(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::NONE)
            mergeNoSampler(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::RNG)
            mergeWithRNGSampler(cur_elems, rhs_elems, arena);
    }

    void mergeNoSamplerLast(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elements = std::min(static_cast<size_t>(max_elems), cur_elems.value.size() + rhs_elems.value.size());
        cur_elems.value.resize(new_elements, arena);
        for (auto & value : rhs_elems.value)
        {
            cur_elems.value[cur_elems.total_values % max_elems] = value;
            ++cur_elems.total_values;
        }
        assert(rhs_elems.total_values >= rhs_elems.value.size());
        cur_elems.total_values += rhs_elems.total_values - rhs_elems.value.size();
    }

    void mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (!limit_num_elems)
        {
            if (rhs_elems.value.size())
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, rhs_elems.value.size(), arena);
        }
        else
        {
            UInt64 elems_to_insert = std::min(static_cast<size_t>(max_elems) - cur_elems.value.size(), rhs_elems.value.size());
            if (elems_to_insert)
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, elems_to_insert, arena);
        }
    }

    void mergeWithRNGSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (rhs_elems.total_values <= max_elems)
        {
            for (size_t i = 0; i < rhs_elems.value.size(); ++i)
                insertWithSampler(cur_elems, rhs_elems.value[i], arena);
        }
        else if (cur_elems.total_values <= max_elems)
        {
            decltype(cur_elems.value) from;
            from.swap(cur_elems.value, arena);
            cur_elems.value.assign(rhs_elems.value.begin(), rhs_elems.value.end(), arena);
            cur_elems.total_values = rhs_elems.total_values;
            for (size_t i = 0; i < from.size(); ++i)
                insertWithSampler(cur_elems, from[i], arena);
        }
        else
        {
            cur_elems.randomShuffle();
            cur_elems.total_values += rhs_elems.total_values;
            for (size_t i = 0; i < max_elems; ++i)
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < rhs_elems.total_values)
                    cur_elems.value[i] = rhs_elems.value[i];
            }
        }
        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));

        if constexpr (Trait::last)
            DB::writeIntBinary<size_t>(this->data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            DB::writeIntBinary<size_t>(this->data(place).total_values, buf);
            WriteBufferFromOwnString rng_buf;
            rng_buf << this->data(place).rng;
            DB::writeStringBinary(rng_buf.str(), buf);
        }

        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (limit_num_elems && unlikely(size > max_elems))
            throw Exception("Too large array size, it should not exceed " + toString(max_elems), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = this->data(place).value;

        value.resize(size, arena);
        buf.readStrict(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));

        if constexpr (Trait::last)
            DB::readIntBinary<size_t>(this->data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            DB::readIntBinary<size_t>(this->data(place).total_values, buf);
            std::string rng_string;
            DB::readStringBinary(rng_string, buf);
            ReadBufferFromString rng_buf(rng_string);
            rng_buf >> this->data(place).rng;
        }

        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};


/// General case


/// Nodes used to implement a linked list for storage of group_array states

template <typename Node>
struct GroupArrayNodeBase
{
    UInt64 size; // size of payload

    /// Returns pointer to actual payload
    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

    /// Clones existing node (does not modify next field)
    Node * clone(Arena * arena) const
    {
        return reinterpret_cast<Node *>(
            const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size, alignof(Node))));
    }

    /// Write node to buffer
    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    /// Reads and allocates node from ReadBuffer's data (doesn't set next)
    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.readStrict(node->data(), size);
        return node;
    }
};

struct GroupArrayNodeString : public GroupArrayNodeBase<GroupArrayNodeString>
{
    using Node = GroupArrayNodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column)
    {
        assert_cast<ColumnString &>(column).insertData(data(), size);
    }
};

struct GroupArrayNodeGeneral : public GroupArrayNodeBase<GroupArrayNodeGeneral>
{
    using Node = GroupArrayNodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
        StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

        Node * node = reinterpret_cast<Node *>(const_cast<char *>(begin));
        node->size = value.size;

        return node;
    }

    void insertInto(IColumn & column) { column.deserializeAndInsertFromArena(data()); }
};

template <typename Node, bool has_sampler>
struct GroupArrayGeneralData;

template <typename Node>
struct GroupArrayGeneralData<Node, false>
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(Node *), 4096>;
    using Array = PODArray<Node *, 32, Allocator>;

    // For group_array_last()
    size_t total_values = 0;
    Array value;
};

template <typename Node>
struct GroupArrayGeneralData<Node, true> : public GroupArraySamplerData<Node *>
{
};

/// Implementation of group_array for String or any ComplexObject via array
template <typename Node, typename Trait>
class GroupArrayGeneralImpl final
    : public IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>, GroupArrayGeneralImpl<Node, Trait>>
{
    static constexpr bool limit_num_elems = Trait::has_limit;
    using Data = GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;
    UInt64 seed;

public:
    GroupArrayGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, Trait::sampler != Sampler::NONE>, GroupArrayGeneralImpl<Node, Trait>>(
            {data_type_}, parameters_)
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return getNameByTrait<Trait>(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(data_type); }

    void insertWithSampler(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        if (a.value.size() < max_elems)
            a.value.push_back(v->clone(arena), arena);
        else
        {
            UInt64 rnd = a.genRandom(a.total_values);
            if (rnd < max_elems)
                a.value[rnd] = v->clone(arena);
        }
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
        if constexpr (Trait::sampler == Sampler::RNG)
            a->rng.seed(seed);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        data(place).~Data();
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data>;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_elems = data(place);

        ++cur_elems.total_values;

        if constexpr (Trait::sampler == Sampler::NONE)
        {
            if (limit_num_elems && cur_elems.value.size() >= max_elems)
            {
                if (Trait::last)
                {
                    Node * node = Node::allocate(*columns[0], row_num, arena);
                    cur_elems.value[(cur_elems.total_values - 1) % max_elems] = node;
                }
                return;
            }

            Node * node = Node::allocate(*columns[0], row_num, arena);
            cur_elems.value.push_back(node, arena);
        }

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            if (cur_elems.value.size() < max_elems)
            {
                cur_elems.value.push_back(Node::allocate(*columns[0], row_num, arena), arena);
            }
            else
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < max_elems)
                    cur_elems.value[rnd] = Node::allocate(*columns[0], row_num, arena);
            }
        }
        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = data(place);
        auto & rhs_elems = data(rhs);

        if (rhs_elems.value.empty())
            return;

        if constexpr (Trait::last)
            mergeNoSamplerLast(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::NONE)
            mergeNoSampler(cur_elems, rhs_elems, arena);
        else if constexpr (Trait::sampler == Sampler::RNG)
            mergeWithRNGSampler(cur_elems, rhs_elems, arena);
        // TODO
        // else if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void ALWAYS_INLINE mergeNoSamplerLast(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elements = std::min(static_cast<size_t>(max_elems), cur_elems.value.size() + rhs_elems.value.size());
        cur_elems.value.resize(new_elements, arena);

        for (auto & value : rhs_elems.value)
        {
            cur_elems.value[cur_elems.total_values % max_elems] = value->clone(arena);
            ++cur_elems.total_values;
        }


        assert(rhs_elems.total_values >= rhs_elems.value.size());
        cur_elems.total_values += rhs_elems.total_values - rhs_elems.value.size();
    }

    void ALWAYS_INLINE mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elems;
        if (limit_num_elems)
        {
            if (cur_elems.value.size() >= max_elems)
                return;
            new_elems = std::min(rhs_elems.value.size(), static_cast<size_t>(max_elems) - cur_elems.value.size());
        }
        else
            new_elems = rhs_elems.value.size();

        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i]->clone(arena), arena);
       
    }

    void ALWAYS_INLINE mergeWithRNGSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        if (rhs_elems.total_values <= max_elems)
        {
            for (size_t i = 0; i < rhs_elems.value.size(); ++i)
                insertWithSampler(cur_elems, rhs_elems.value[i], arena);
        }
        else if (cur_elems.total_values <= max_elems)
        {
            decltype(cur_elems.value) from;
            from.swap(cur_elems.value, arena);
            for (auto & node : rhs_elems.value)
                cur_elems.value.push_back(node->clone(arena), arena);

            cur_elems.total_values = rhs_elems.total_values;
            for (size_t i = 0; i < from.size(); ++i)
                insertWithSampler(cur_elems, from[i], arena);
        }
        else
        {
            cur_elems.randomShuffle();
            cur_elems.total_values += rhs_elems.total_values;
            for (size_t i = 0; i < max_elems; ++i)
            {
                UInt64 rnd = cur_elems.genRandom(cur_elems.total_values);
                if (rnd < rhs_elems.total_values)
                    cur_elems.value[i] = rhs_elems.value[i]->clone(arena);

            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).value.size(), buf);

        auto & value = data(place).value;
        for (auto & node : value)
            node->write(buf);

        if constexpr (Trait::last)
            DB::writeIntBinary<size_t>(data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            DB::writeIntBinary<size_t>(data(place).total_values, buf);
            WriteBufferFromOwnString rng_buf;
            rng_buf << data(place).rng;
            DB::writeStringBinary(rng_buf.str(), buf);
        }

        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (limit_num_elems && unlikely(elems > max_elems))
            throw Exception("Too large array size, it should not exceed " + toString(max_elems), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = data(place).value;

        value.resize(elems, arena);
        for (UInt64 i = 0; i < elems; ++i)
            value[i] = Node::read(buf, arena);

        if constexpr (Trait::last)
            DB::readIntBinary<size_t>(data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            DB::readIntBinary<size_t>(data(place).total_values, buf);
            std::string rng_string;
            DB::readStringBinary(rng_string, buf);
            ReadBufferFromString rng_buf(rng_string);
            rng_buf >> data(place).rng;
        }

        // TODO
        // if constexpr (Trait::sampler == Sampler::DETERMINATOR)
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);

        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + data(place).value.size());

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArrayNodeString>)
        {
            auto & string_offsets = assert_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + data(place).value.size());
        }

        auto & value = data(place).value;
        for (auto & node : value)
            node->insertInto(column_data);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

template <typename Node>
struct GroupArrayListNodeBase : public GroupArrayNodeBase<Node>
{
    Node * next;
};

struct GroupArrayListNodeString : public GroupArrayListNodeBase<GroupArrayListNodeString>
{
    using Node = GroupArrayListNodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
        node->next = nullptr;
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column) { assert_cast<ColumnString &>(column).insertData(data(), size); }
};

struct GroupArrayListNodeGeneral : public GroupArrayListNodeBase<GroupArrayListNodeGeneral>
{
    using Node = GroupArrayListNodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
        StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

        Node * node = reinterpret_cast<Node *>(const_cast<char *>(begin));
        node->next = nullptr;
        node->size = value.size;

        return node;
    }

    void insertInto(IColumn & column) { column.deserializeAndInsertFromArena(data()); }
};


template <typename Node>
struct GroupArrayGeneralListData
{
    UInt64 elems = 0;
    Node * first = nullptr;
    Node * last = nullptr;
};


/// Implementation of group_array for String or any ComplexObject via linked list
/// It has poor performance in case of many small objects
template <typename Node, typename Trait>
class GroupArrayGeneralListImpl final
    : public IAggregateFunctionDataHelper<GroupArrayGeneralListData<Node>, GroupArrayGeneralListImpl<Node, Trait>>
{
    static constexpr bool limit_num_elems = Trait::has_limit;
    using Data = GroupArrayGeneralListData<Node>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;

public:
    GroupArrayGeneralListImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<GroupArrayGeneralListData<Node>, GroupArrayGeneralListImpl<Node, Trait>>({data_type_}, parameters_)
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return getNameByTrait<Trait>(); }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(data_type); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (limit_num_elems && data(place).elems >= max_elems)
            return;

        Node * node = Node::allocate(*columns[0], row_num, arena);

        if (unlikely(!data(place).first))
        {
            data(place).first = node;
            data(place).last = node;
        }
        else
        {
            data(place).last->next = node;
            data(place).last = node;
        }

        ++data(place).elems;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        /// It is sadly, but rhs's Arena could be destroyed

        if (!data(rhs).first) /// rhs state is empty
            return;

        UInt64 new_elems;
        UInt64 cur_elems = data(place).elems;
        if (limit_num_elems)
        {
            if (data(place).elems >= max_elems)
                return;

            new_elems = std::min(data(place).elems + data(rhs).elems, static_cast<size_t>(max_elems));
        }
        else
        {
            new_elems = data(place).elems + data(rhs).elems;
        }

        Node * p_rhs = data(rhs).first;
        Node * p_lhs;

        if (unlikely(!data(place).last)) /// lhs state is empty
        {
            p_lhs = p_rhs->clone(arena);
            data(place).first = data(place).last = p_lhs;
            p_rhs = p_rhs->next;
            ++cur_elems;
        }
        else
        {
            p_lhs = data(place).last;
        }

        for (; cur_elems < new_elems; ++cur_elems)
        {
            Node * p_new = p_rhs->clone(arena);
            p_lhs->next = p_new;
            p_rhs = p_rhs->next;
            p_lhs = p_new;
        }

        p_lhs->next = nullptr;
        data(place).last = p_lhs;
        data(place).elems = new_elems;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).elems, buf);

        Node * p = data(place).first;
        while (p)
        {
            p->write(buf);
            p = p->next;
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);
        data(place).elems = elems;

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (limit_num_elems && unlikely(elems > max_elems))
            throw Exception("Too large array size, it should not exceed " + toString(max_elems), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        Node * prev = Node::read(buf, arena);
        data(place).first = prev;

        for (UInt64 i = 1; i < elems; ++i)
        {
            Node * cur = Node::read(buf, arena);
            prev->next = cur;
            prev = cur;
        }

        prev->next = nullptr;
        data(place).last = prev;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);

        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + data(place).elems);

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArrayListNodeString>)
        {
            auto & string_offsets = assert_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + data(place).elems);
        }

        Node * p = data(place).first;
        while (p)
        {
            p->insertInto(column_data);
            p = p->next;
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
