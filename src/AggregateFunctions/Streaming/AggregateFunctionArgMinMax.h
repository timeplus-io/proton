#pragma once

#include "config.h"

#if USE_ARG_MIN_MAX_FUNCS

#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{
/// Returns the first arg value found for the minimum/maximum value. Example: arg_max(arg, value).
/// There are similar problems and trade off as stated in AggregateFunctionsCountedValue in AggregateFunctionMinMaxAny.h
/// In changelog mode, we need not only keep track the unique min/max value, but also need keep track the unique argument
/// for each unique value. The following is one example how we keep tracking this in data structure
/// For max sequence (value, arg, delta) and `retract_max = 3`:
/// (10, 'a', 1), (10, 'b', 1), (9, 'c', 1), (8, 'd', 1), (10, 'a', -1), (10, 'b', -1), (9, 'c', -1), (7, 'e', 1)
/// value -> [{arg, count}, ...]
/// 10 -> [('a', 1), ('b', 1)]
/// 9 -> [('c', 1)]
/// When 9 gets inserted, we reach the max retract size which is 3 (note for value 10, there are 2 different args),
/// so (8, 'd', 1) gets dropped on the floor.
/// After we dropped 8 on the floor, 10, 9 get retracted, and the map is empty.
/// Then (7, 'e', 1) gets inserted into the map, at this very point of time, 7 will be the maximum value which
/// is wrong as discussed in AggregateFunctionsCountedValue.
/// Note arg min/max is a bit more complex than simple min/max as the `arg` can be different for the same min/max values
template <typename Data>
class AggregateFunctionArgMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>
{
private:
    const DataTypePtr & type_res;
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;
    UInt64 max_size;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_, const Settings * settings)
        : Base({type_res_, type_val_}, {})
        , type_res(this->argument_types[0])
        , type_val(this->argument_types[1])
        , serialization_res(type_res->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
        , max_size(settings->retract_max.value)
    {
        if (!type_res->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of aggregate function {} because the values of that data type are not comparable",
                type_res->getName(), getName());

        if (!type_val->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of aggregate function {} because the values of that data type are not comparable",
                type_val->getName(), getName());
    }

    void create(AggregateDataPtr place) const override { new (place) Data(static_cast<int64_t>(max_size)); }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override { return type_res; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(*columns[0], *columns[1], row_num, arena);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const final
    {
        this->data(place).negate(*columns[0], *columns[1], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization_res, *serialization_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization_res, *serialization_val, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};
}
}

#endif
