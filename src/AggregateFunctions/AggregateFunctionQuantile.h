#pragma once

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/QuantilesCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>
#include <Interpreters/GatherFunctionQuantileVisitor.h>

#include <type_traits>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename> class QuantileTiming;

/// proton: starts.
struct NameP90 { static constexpr auto name = "p90"; };
struct NameP95 { static constexpr auto name = "p95"; };
struct NameP99 { static constexpr auto name = "p99"; };
/// proton: ends.

/** Generic aggregate function for calculation of quantiles.
  * It depends on quantile calculation data structure. Look at Quantile*.h for various implementations.
  */

template <
    /// Type of first argument.
    typename Value,
    /// Data structure and implementation of calculation. Look at QuantileExact.h for example.
    typename Data,
    /// Structure with static member "name", containing the name of the aggregate function.
    typename Name,
    /// If true, the function accepts the second argument
    /// (in can be "weight" to calculate quantiles or "determinator" that is used instead of PRNG).
    /// Second argument is always obtained through 'getUInt' method.
    bool has_second_arg,
    /// If non-void, the function will return float of specified type with possibly interpolated results and NaN if there was no values.
    /// Otherwise it will return Value type and default value if there was no values.
    /// As an example, the function cannot return floats, if the SQL type of argument is Date or DateTime.
    typename FloatReturnType,
    /// If true, the function will accept multiple parameters with quantile levels
    ///  and return an Array filled with many values of that quantiles.
    bool returns_many>
class AggregateFunctionQuantile final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many>>
{
private:
    using ColVecType = ColumnVectorOrDecimal<Value>;

    static constexpr bool returns_float = !(std::is_same_v<FloatReturnType, void>);
    static_assert(!is_decimal<Value> || !returns_float);

    QuantileLevels<Float64> levels;

    /// Used when there are single level to get.
    Float64 level = 0.5;

    DataTypePtr & argument_type;

public:
    AggregateFunctionQuantile(const DataTypes & argument_types_, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionQuantile<Value, Data, Name, has_second_arg, FloatReturnType, returns_many>>(
            argument_types_, params)
        , levels(params, returns_many)
        , level(levels.levels[0])
        , argument_type(this->argument_types[0])
    {
        /// proton: starts. specialization of quantile alias functions: 'p90' 'p95' 'p99'
        if constexpr (std::is_same_v<Name, NameP90> || std::is_same_v<Name, NameP95> || std::is_same_v<Name, NameP99>)
        {
            assert(this->levels.size() == 1);  /// default: 0.5
            /// Make level: 'p90' -> 0.90  'p95' -> 0.95  'p99' -> 0.99
            this->level = this->levels.levels[0] = std::stod(fmt::format("0.{}", this->getName().substr(1)));
        }
        /// proton: ends.

        if (!returns_many && levels.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} require one parameter or less", getName());
    }

    String getName() const override { return Name::name; }

    DataTypePtr getReturnType() const override
    {
        DataTypePtr res;

        if constexpr (returns_float)
            res = std::make_shared<DataTypeNumber<FloatReturnType>>();
        else
            res = argument_type;

        if constexpr (returns_many)
            return std::make_shared<DataTypeArray>(res);
        else
            return res;
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return GatherFunctionQuantileData::toFusedNameOrSelf(getName()) == GatherFunctionQuantileData::toFusedNameOrSelf(rhs.getName())
            && this->haveEqualArgumentTypes(rhs);
    }

    DataTypePtr getNormalizedStateType() const override
    {
        /// Return normalized state type: quantiles*(1)(...)
        Array params{1};
        AggregateFunctionProperties properties;
        return std::make_shared<DataTypeAggregateFunction>(
            AggregateFunctionFactory::instance().get(
                GatherFunctionQuantileData::toFusedNameOrSelf(getName()), this->argument_types, params, properties),
            this->argument_types,
            params);
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = static_cast<const ColVecType &>(*columns[0]).getData()[row_num];

        if constexpr (std::is_same_v<Data, QuantileTiming<Value>>)
        {
            /// QuantileTiming only supports unsigned integers. Too large values are also meaningless.
            if (isNaN(value) || value > std::numeric_limits<Int64>::max() || value < 0)
                return;
        }

        if constexpr (has_second_arg)
            this->data(place).add(value, columns[1]->getUInt(row_num));
        else
            this->data(place).add(value);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        /// const_cast is required because some data structures apply finalizaton (like compactization) before serializing.
        this->data(const_cast<AggregateDataPtr>(place)).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);

        if constexpr (returns_many)
        {
            ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
            ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

            size_t size = levels.size();
            offsets_to.push_back(offsets_to.back() + size);

            if (!size)
                return;

            if constexpr (returns_float)
            {
                auto & data_to = assert_cast<ColumnVector<FloatReturnType> &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(old_size + size);

                data.getManyFloat(levels.levels.data(), levels.permutation.data(), size, data_to.data() + old_size);
            }
            else
            {
                auto & data_to = static_cast<ColVecType &>(arr_to.getData()).getData();
                size_t old_size = data_to.size();
                data_to.resize(old_size + size);

                data.getMany(levels.levels.data(), levels.permutation.data(), size, data_to.data() + old_size);
            }
        }
        else
        {
            if constexpr (returns_float)
                assert_cast<ColumnVector<FloatReturnType> &>(to).getData().push_back(data.getFloat(level));
            else
                static_cast<ColVecType &>(to).getData().push_back(data.get(level));
        }
    }

    static void assertSecondArg(const DataTypes & types)
    {
        if constexpr (has_second_arg)
        {
            assertBinary(Name::name, types);
            if (!isUnsignedInteger(types[1]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument (weight) for function {} must be unsigned integer, but it has type {}",
                    Name::name,
                    types[1]->getName());
        }
        else
            assertUnary(Name::name, types);
    }
};

struct NameQuantile { static constexpr auto name = "quantile"; };
struct NameQuantiles { static constexpr auto name = "quantiles"; };
struct NameQuantileDeterministic { static constexpr auto name = "quantile_deterministic"; };
struct NameQuantilesDeterministic { static constexpr auto name = "quantiles_deterministic"; };

struct NameQuantileExact { static constexpr auto name = "quantile_exact"; };
struct NameQuantilesExact { static constexpr auto name = "quantiles_exact"; };

struct NameQuantileExactLow { static constexpr auto name = "quantile_exact_low"; };
struct NameQuantilesExactLow { static constexpr auto name = "quantiles_exact_low"; };

struct NameQuantileExactHigh { static constexpr auto name = "quantile_exact_high"; };
struct NameQuantilesExactHigh { static constexpr auto name = "quantiles_exact_high"; };

struct NameQuantileExactExclusive { static constexpr auto name = "quantile_exact_exclusive"; };
struct NameQuantilesExactExclusive { static constexpr auto name = "quantiles_exact_exclusive"; };

struct NameQuantileExactInclusive { static constexpr auto name = "quantile_exact_inclusive"; };
struct NameQuantilesExactInclusive { static constexpr auto name = "quantiles_exact_inclusive"; };

struct NameQuantileExactWeighted { static constexpr auto name = "quantile_exact_weighted"; };
struct NameQuantilesExactWeighted { static constexpr auto name = "quantiles_exact_weighted"; };

struct NameQuantileTiming { static constexpr auto name = "quantile_timing"; };
struct NameQuantileTimingWeighted { static constexpr auto name = "quantile_timing_weighted"; };
struct NameQuantilesTiming { static constexpr auto name = "quantiles_timing"; };
struct NameQuantilesTimingWeighted { static constexpr auto name = "quantiles_timing_weighted"; };

struct NameQuantileTDigest { static constexpr auto name = "quantile_t_digest"; };
struct NameQuantileTDigestWeighted { static constexpr auto name = "quantile_t_digest_weighted"; };
struct NameQuantilesTDigest { static constexpr auto name = "quantiles_t_digest"; };
struct NameQuantilesTDigestWeighted { static constexpr auto name = "quantiles_t_digest_weighted"; };

struct NameQuantileBFloat16 { static constexpr auto name = "quantile_b_float16"; };
struct NameQuantilesBFloat16 { static constexpr auto name = "quantiles_b_float16"; };
struct NameQuantileBFloat16Weighted { static constexpr auto name = "quantile_b_float16_weighted"; };
struct NameQuantilesBFloat16Weighted { static constexpr auto name = "quantiles_b_float16_weighted"; };

}
