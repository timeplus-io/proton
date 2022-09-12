#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/assert_cast.h>

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

#include <numeric>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Streaming
{
struct AggregateFunctionCountData
{
    UInt64 count = 0;
};

/// Simply count number of calls.
class AggregateFunctionCount final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    AggregateFunctionCount(const DataTypes & argument_types_) : IAggregateFunctionDataHelper(argument_types_, {}) { }

    String getName() const override { return "__count_retract"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override { ++data(place).count; }

    void negate(AggregateDataPtr __restrict place, const IColumn ** , size_t, Arena *) const override { --data(place).count; }

    void addBatchSinglePlace(
        size_t /*batch_size*/,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col) const override
    {
        assert(delta_col != nullptr);

        const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
        auto & count = data(place).count;

        /// FIXME, vectorize this
        /// countBytesInFilter();
        /// countBytesInFilterWithNull

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnBool &>(*columns[if_argument_pos]).getData();

            for (size_t idx = 0; auto filter : flags)
            {
                if (filter != 0)
                     count += delta_flags[idx];

                ++idx;
            }
        }
        else
        {
            /// Sum of delta_flag
            count = std::accumulate(delta_flags.begin(), delta_flags.end(), count);
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col) const override
    {
        assert(delta_col != nullptr);

        const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
        auto & count = data(place).count;

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnBool &>(*columns[if_argument_pos]).getData();
            for (size_t idx = 0; idx < batch_size; ++idx)
                if (flags[idx] && null_map[idx] == 0)
                    count += delta_flags[idx];

            /// count += countBytesInFilterWithNull(flags, null_map);
        }
        else
        {
            for (size_t idx = 0; idx < batch_size; ++idx)
                if (null_map[idx] == 0)
                    count += delta_flags[idx];
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

    /// Reset the state to specified value. This function is not the part of common interface.
    void set(AggregateDataPtr __restrict place, Int64 new_count) const { data(place).count = new_count; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr &,
        const DataTypes & types,
        const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override;

#if 0 /// FIXME, JIT
#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        bool is_compilable = true;
        for (const auto & argument_type : argument_types)
            is_compilable &= canBeNativeType(*argument_type);

        return is_compilable;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(
            aggregate_data_ptr,
            llvm::ConstantInt::get(b.getInt8Ty(), 0),
            sizeof(AggregateFunctionCountData),
            llvm::assumeAligned(this->alignOfData()));
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes &, const std::vector<llvm::Value *> &)
        const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * count_value = b.CreateLoad(return_type, count_value_ptr);
        auto * updated_count_value = b.CreateAdd(count_value, llvm::ConstantInt::get(return_type, 1));

        b.CreateStore(updated_count_value, count_value_ptr);
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * count_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

        auto * count_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

        auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

        b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, count_value_ptr);
    }

#endif
#endif
};


/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final
    : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>
{
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr & argument, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>({argument}, params)
    {
        if (!argument->isNullable())
            throw Exception(
                "Logical error: not Nullable data type passed to AggregateFunctionCountNotNullUnary", ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "__count_retract"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).count += !assert_cast<const ColumnNullable &>(*columns[0]).isNullAt(row_num);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).count -= !assert_cast<const ColumnNullable &>(*columns[0]).isNullAt(row_num);
    }

    void addBatchSinglePlace(
        size_t batch_size,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos,
        const IColumn * delta_col) const override
    {
        assert(delta_col != nullptr);

        const auto & delta_flags = assert_cast<const ColumnInt8 &>(*delta_col).getData();
        auto & count = data(place).count;
        auto & nc = assert_cast<const ColumnNullable &>(*columns[0]);

        /// FIXME, vectorize this
        /// countBytesInFilterWithNull

        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnBool &>(*columns[if_argument_pos]).getData();

            const auto & null_map = nc.getNullMapData();

            for (size_t i = 0; i < batch_size; ++i)
                if (flags[i] != 0 && null_map[i] == 0)
                     count += delta_flags[i];

            /// data(place).count += countBytesInFilterWithNull(flags, nc.getNullMapData().data());
        }
        else
        {
            const auto & null_map = nc.getNullMapData();

            for (size_t i = 0; i < batch_size; ++i)
                if (!null_map[i])
                    count += delta_flags[i];

            /// data(place).count += batch_size - countBytesInFilter(nc.getNullMapData().data(), batch_size);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

#if 0 /// FIXME jit
#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        bool is_compilable = true;
        for (const auto & argument_type : argument_types)
            is_compilable &= canBeNativeType(*argument_type);


        return is_compilable;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(
            aggregate_data_ptr,
            llvm::ConstantInt::get(b.getInt8Ty(), 0),
            sizeof(AggregateFunctionCountData),
            llvm::assumeAligned(this->alignOfData()));
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes &,
        const std::vector<llvm::Value *> & values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * is_null_value = b.CreateExtractValue(values[0], {1});
        auto * increment_value
            = b.CreateSelect(is_null_value, llvm::ConstantInt::get(return_type, 0), llvm::ConstantInt::get(return_type, 1));

        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * count_value = b.CreateLoad(return_type, count_value_ptr);
        auto * updated_count_value = b.CreateAdd(count_value, increment_value);

        b.CreateStore(updated_count_value, count_value_ptr);
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * count_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * count_value_dst = b.CreateLoad(return_type, count_value_dst_ptr);

        auto * count_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * count_value_src = b.CreateLoad(return_type, count_value_src_ptr);

        auto * count_value_dst_updated = b.CreateAdd(count_value_dst, count_value_src);

        b.CreateStore(count_value_dst_updated, count_value_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * count_value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, count_value_ptr);
    }

#endif
#endif
};
}
}
