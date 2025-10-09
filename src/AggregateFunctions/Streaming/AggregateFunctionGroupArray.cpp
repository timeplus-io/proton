#include <AggregateFunctions/Streaming/CountedValueHashMap.h>
#include <AggregateFunctions/Streaming/CountedValueMap.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace Streaming
{
namespace
{
template <bool IsSorted, bool IsUniqued, bool IsGenericData = false, bool IsSerializedData = false>
struct GroupArrayTrait
{
    static constexpr bool is_sorted = IsSorted;
    static constexpr bool is_uniqued = IsUniqued;

    static constexpr bool is_generic_data = IsGenericData;
    static constexpr bool is_serialized_data = IsSerializedData;
};

template <typename T, bool is_sorted>
struct GroupArrayData
{
    using Map = std::conditional_t<is_sorted, CountedValueMap<T, false>, CountedValueHashMap<T>>;
    Map map;

    GroupArrayData() = default;
    GroupArrayData(size_t max_elements) : map(max_elements) { }
};

template <typename T, typename Trait>
class GroupArrayImpl final : public IAggregateFunctionDataHelper<GroupArrayData<T, Trait::is_sorted>, GroupArrayImpl<T, Trait>>
{
public:
    using Data = GroupArrayData<T, Trait::is_sorted>;

    explicit GroupArrayImpl(const DataTypes & argument_types_, const Array & parameters_, size_t max_elements_)
        : IAggregateFunctionDataHelper<Data, GroupArrayImpl<T, Trait>>(
              argument_types_, parameters_, std::make_shared<DataTypeArray>(argument_types_[0]))
        , max_elements(max_elements_)
    {
    }

    String getName() const override
    {
        if constexpr (Trait::is_sorted)
            return "group_array_sorted";
        else if constexpr (Trait::is_uniqued)
            return "group_uniq_array";
        else
            return "group_array";
    }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override { new (place) Data(max_elements); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (Trait::is_generic_data)
        {
            this->data(place).map.emplace(getKeyHolder</*is_plain_column=*/!Trait::is_serialized_data>(*columns[0], row_num));
        }
        else
        {
            auto row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
            this->data(place).map.emplace(std::move(row_value));
        }
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (Trait::is_generic_data)
        {
            this->data(place).map.erase(getKeyHolder</*is_plain_column=*/!Trait::is_serialized_data>(*columns[0], row_num));
        }
        else
        {
            const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
            this->data(place).map.erase(row_value);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).map.merge(this->data(rhs).map);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).map;
        size_t size = map.size();
        writeVarUInt(size, buf);
        for (const auto & [element, count] : map)
        {
            writeBinary(element, buf);
            writeVarUInt(count, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > max_elements))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elements);

        auto & map = this->data(place).map;

        T val;
        uint32_t count = 0;
        for (size_t i = 0; i < size; i++)
        {
            if constexpr (std::is_same_v<T, StringRef>)
                val = readStringBinaryInto(*this->data(place).map.getArena().getArenaWithFreeLists(), buf);
            else
                readBinary(val, buf);

            readVarUInt(count, buf);
            map.template insert</*preallocated=*/true>(std::move(val), count);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        const auto & map = this->data(place).map;

        size_t total_size = 0;
        if constexpr (Trait::is_uniqued)
        {
            total_size = map.size();
            chassert(total_size <= max_elements);
        }
        else
        {
            for (const auto & [_, count] : map)
                total_size += count;

            total_size = std::min(total_size, max_elements);
        }

        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        offsets_to.push_back(offsets_to.back() + total_size);

        if constexpr (Trait::is_generic_data)
        {
            IColumn & data_to = arr_to.getData();
            data_to.reserve(data_to.size() + total_size);

            if constexpr (Trait::is_uniqued)
            {
                for (const auto & [val, _] : map)
                    deserializeAndInsert</*is_plain_column=*/!Trait::is_serialized_data>(val, data_to);
            }
            else
            {
                for (const auto & [val, count] : map)
                {
                    if (total_size >= count) [[likely]]
                    {
                        for (size_t j = 0; j < count; ++j)
                            deserializeAndInsert</*is_plain_column=*/!Trait::is_serialized_data>(val, data_to);

                        total_size -= count;
                    }
                    else
                    {
                        /// Reached the max_elements limit
                        for (size_t j = 0; j < total_size; ++j)
                            deserializeAndInsert</*is_plain_column=*/!Trait::is_serialized_data>(val, data_to);

                        return;
                    }
                }
            }
        }
        else
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            size_t i = data_to.size();
            data_to.resize(i + total_size);
            if constexpr (Trait::is_uniqued)
            {
                for (const auto & [val, _] : map)
                    data_to[i++] = val;
            }
            else
            {
                for (const auto & [val, count] : map)
                {
                    if (total_size >= count) [[likely]]
                    {
                        for (size_t j = 0; j < count; ++j)
                            data_to[i++] = val;

                        total_size -= count;
                    }
                    else
                    {
                        /// Reached the max_elements limit
                        for (size_t j = 0; j < total_size; ++j)
                            data_to[i++] = val;

                        return;
                    }
                }
            }
        }
    }

private:
    size_t max_elements;
};

template <bool is_sorted, bool is_uniqued>
AggregateFunctionPtr createAggregateFunctionGroupArrayRetractImpl(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, size_t max_elems)
{
    const auto & argument_type = *argument_types[0];
    if (auto res = AggregateFunctionPtr(createWithNumericType<GroupArrayImpl, GroupArrayTrait<is_sorted, is_uniqued>>(
            argument_type, argument_types, parameters, max_elems));
        res)
        return res;

    WhichDataType which(argument_type);
    switch (which.idx)
    {
        case TypeIndex::Date:
            return std::make_shared<GroupArrayImpl<DataTypeDate::FieldType, GroupArrayTrait<is_sorted, is_uniqued>>>(
                argument_types, parameters, max_elems);
        case TypeIndex::DateTime:
            return std::make_shared<GroupArrayImpl<DataTypeDateTime::FieldType, GroupArrayTrait<is_sorted, is_uniqued>>>(
                argument_types, parameters, max_elems);
        case TypeIndex::IPv4:
            return std::make_shared<GroupArrayImpl<IPv4, GroupArrayTrait<is_sorted, is_uniqued>>>(argument_types, parameters, max_elems);
        default:
        {
            /// Check that we can use plain version of GroupArrayGenericImpl
            if (argument_type.isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return std::make_shared<GroupArrayImpl<
                    StringRef,
                    GroupArrayTrait<is_sorted, is_uniqued, /*is_generic_data=*/true, /*is_serialized_data=*/false>>>(
                    argument_types, parameters, max_elems);
            else
                return std::make_shared<GroupArrayImpl<
                    StringRef,
                    GroupArrayTrait<is_sorted, is_uniqued, /*is_generic_data=*/true, /*is_serialized_data=*/true>>>(
                    argument_types, parameters, max_elems);
        }
    }
}

AggregateFunctionPtr createAggregateFunctionGroupArrayRetract(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    /// group_array(x, _tp_delta)
    assertBinary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();
    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        max_elems = parameters[0].get<UInt64>();
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1",
            name);
    }

    return createAggregateFunctionGroupArrayRetractImpl</*is_sorted=*/false, /*is_uniqued=*/false>(
        name, argument_types, parameters, max_elems);
}

AggregateFunctionPtr createAggregateFunctionGroupArraySortedRetract(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    /// group_array_sorted(x, _tp_delta)
    assertBinary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();
    if (parameters.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should have limit argument", name);
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        max_elems = parameters[0].get<UInt64>();
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1",
            name);
    }

    return createAggregateFunctionGroupArrayRetractImpl</*is_sorted=*/true, /*is_uniqued=*/false>(
        name, argument_types, parameters, max_elems);
}

AggregateFunctionPtr createAggregateFunctionGroupUniqArrayRetract(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    /// group_uniq_array(x, _tp_delta)
    assertBinary(name, argument_types);

    UInt64 max_elems = std::numeric_limits<UInt64>::max();
    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

        max_elems = parameters[0].get<UInt64>();
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1",
            name);
    }

    return createAggregateFunctionGroupArrayRetractImpl</*is_sorted=*/false, /*is_uniqued=*/true>(
        name, argument_types, parameters, max_elems);
}

}

void registerAggregateFunctionGroupArrayRetract(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = false};

    factory.registerFunction("__group_array_retract", {createAggregateFunctionGroupArrayRetract, properties});
    factory.registerFunction("__group_uniq_array_retract", {createAggregateFunctionGroupUniqArrayRetract, properties});
    factory.registerFunction("__group_array_sorted_retract", {createAggregateFunctionGroupArraySortedRetract, properties});
}

}
}
