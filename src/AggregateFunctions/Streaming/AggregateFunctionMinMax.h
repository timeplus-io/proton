#pragma once

#include "CountedValueMap.h"

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/StringRef.h>
#include <Common/assert_cast.h>

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NOT_IMPLEMENTED;
}

namespace Streaming
{
/** Aggregate functions that store one of passed values.
  * For example: min, max, any, any_last.
  */

/// For numeric values.
template <typename T, bool maximum>
struct CountedValuesDataFixed
{
private:
    using Self = CountedValuesDataFixed;
    using ColVecType = ColumnVectorOrDecimal<T>;

    CountedValueMap<T, maximum> values;

public:
    CountedValuesDataFixed() : CountedValuesDataFixed(1) { }
    explicit CountedValuesDataFixed(int64_t size) : values(size) { }

    static constexpr bool is_nullable = false;

    bool has() const { return !values.empty(); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            assert_cast<ColVecType &>(to).getData().push_back(values.firstValue());
        else
            assert_cast<ColVecType &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
    {
        /// Write size first
        writeVarInt(values.size(), buf);

        /// Then write value / count
        for (auto [val, count] : values)
        {
            writeBinary(val, buf);
            writeVarUInt(count, buf);
        }
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena *)
    {
        Int64 size = 0;
        readVarInt(size, buf);

        assert(size >= 0);

        values.setCapacity(std::max(size, values.capacity()));

        for (Int64 i = 0; i < size; ++i)
        {
            T value;
            UInt32 count;

            readBinary(value, buf);
            readVarUInt(count, buf);

            [[maybe_unused]] auto inserted = values.insert(value, count);
            assert(inserted);
        }
    }

    bool add(const IColumn & column, size_t row_num, Arena *)
    {
        return values.insert(assert_cast<const ColVecType &>(column).getData()[row_num]);
    }

    bool negate(const IColumn & column, size_t row_num, Arena *)
    {
        return values.erase(assert_cast<const ColVecType &>(column).getData()[row_num]);
    }

    bool merge(const Self & rhs, Arena *)
    {
        values.merge(rhs.values);
        return true;
    }

    static bool allocatesMemoryInArena() { return false; }

#if 0
#    if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = true;

    static llvm::Value * getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        static constexpr size_t value_offset_from_structure = offsetof(CountedValuesDataFixed<T>, value);

        auto * type = toNativeType<T>(builder);
        auto * value_ptr_with_offset = b.CreateConstInBoundsGEP1_64(nullptr, aggregate_data_ptr, value_offset_from_structure);
        auto * value_ptr = b.CreatePointerCast(value_ptr_with_offset, type->getPointerTo());

        return value_ptr;
    }

    static llvm::Value * getValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * type = toNativeType<T>(builder);
        auto * value_ptr = getValuePtrFromAggregateDataPtr(builder, aggregate_data_ptr);

        return b.CreateLoad(type, value_ptr);
    }

    static void compileChange(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        b.CreateStore(b.getInt1(true), has_value_ptr);

        auto * value_ptr = getValuePtrFromAggregateDataPtr(b, aggregate_data_ptr);
        b.CreateStore(value_to_check, value_ptr);
    }

    static void
    compileChangeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        auto * value_src = getValueFromAggregateDataPtr(builder, aggregate_data_src_ptr);

        compileChange(builder, aggregate_data_dst_ptr, value_src);
    }

    static void compileChangeFirstTime(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_value = b.CreateLoad(b.getInt1Ty(), has_value_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(has_value_value, if_should_not_change, if_should_change);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_change);
        compileChange(builder, aggregate_data_ptr, value_to_check);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void
    compileChangeFirstTimeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_dst = b.CreateLoad(b.getInt1Ty(), has_value_dst_ptr);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(b.CreateAnd(b.CreateNot(has_value_dst), has_value_src), if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void compileChangeEveryTime(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        compileChange(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeEveryTimeMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        b.CreateCondBr(has_value_src, if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    template <bool is_less>
    static void compileChangeComparison(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_ptr = b.CreatePointerCast(aggregate_data_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_value = b.CreateLoad(b.getInt1Ty(), has_value_ptr);

        auto * value = getValueFromAggregateDataPtr(b, aggregate_data_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        auto is_signed = std::numeric_limits<T>::is_signed;

        llvm::Value * should_change_after_comparison = nullptr;

        if constexpr (is_less)
        {
            if (value_to_check->getType()->isIntegerTy())
                should_change_after_comparison
                    = is_signed ? b.CreateICmpSLT(value_to_check, value) : b.CreateICmpULT(value_to_check, value);
            else
                should_change_after_comparison = b.CreateFCmpOLT(value_to_check, value);
        }
        else
        {
            if (value_to_check->getType()->isIntegerTy())
                should_change_after_comparison
                    = is_signed ? b.CreateICmpSGT(value_to_check, value) : b.CreateICmpUGT(value_to_check, value);
            else
                should_change_after_comparison = b.CreateFCmpOGT(value_to_check, value);
        }

        b.CreateCondBr(b.CreateOr(b.CreateNot(has_value_value), should_change_after_comparison), if_should_change, if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChange(builder, aggregate_data_ptr, value_to_check);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    template <bool is_less>
    static void
    compileChangeComparisonMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * has_value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_dst = b.CreateLoad(b.getInt1Ty(), has_value_dst_ptr);

        auto * value_dst = getValueFromAggregateDataPtr(b, aggregate_data_dst_ptr);

        auto * has_value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, b.getInt1Ty()->getPointerTo());
        auto * has_value_src = b.CreateLoad(b.getInt1Ty(), has_value_src_ptr);

        auto * value_src = getValueFromAggregateDataPtr(b, aggregate_data_src_ptr);

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());
        auto * if_should_not_change = llvm::BasicBlock::Create(head->getContext(), "if_should_not_change", head->getParent());

        auto is_signed = std::numeric_limits<T>::is_signed;

        llvm::Value * should_change_after_comparison = nullptr;

        if constexpr (is_less)
        {
            if (value_src->getType()->isIntegerTy())
                should_change_after_comparison = is_signed ? b.CreateICmpSLT(value_src, value_dst) : b.CreateICmpULT(value_src, value_dst);
            else
                should_change_after_comparison = b.CreateFCmpOLT(value_src, value_dst);
        }
        else
        {
            if (value_src->getType()->isIntegerTy())
                should_change_after_comparison = is_signed ? b.CreateICmpSGT(value_src, value_dst) : b.CreateICmpUGT(value_src, value_dst);
            else
                should_change_after_comparison = b.CreateFCmpOGT(value_src, value_dst);
        }

        b.CreateCondBr(
            b.CreateAnd(has_value_src, b.CreateOr(b.CreateNot(has_value_dst), should_change_after_comparison)),
            if_should_change,
            if_should_not_change);

        b.SetInsertPoint(if_should_change);
        compileChangeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_should_not_change);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    static void compileChangeIfLess(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        static constexpr bool is_less = true;
        compileChangeComparison<is_less>(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfLessMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        static constexpr bool is_less = true;
        compileChangeComparisonMerge<is_less>(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    static void compileChangeIfGreater(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        static constexpr bool is_less = false;
        compileChangeComparison<is_less>(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfGreaterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        static constexpr bool is_less = false;
        compileChangeComparisonMerge<is_less>(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    static llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
    {
        return getValueFromAggregateDataPtr(builder, aggregate_data_ptr);
    }

#    endif
#endif
};

template <bool maximum>
struct CountedValuesDataString
{
private:
    using Self = CountedValuesDataString;
    using ColVecType = ColumnString;

    CountedValueMap<String, maximum> values;

public:
    CountedValuesDataString() : CountedValuesDataString(1) { }
    explicit CountedValuesDataString(int64_t size) : values(size) { }

    static constexpr bool is_nullable = false;

    bool has() const { return !values.empty(); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
        {
            const auto & v = values.firstValue();
            assert_cast<ColVecType &>(to).insertDataWithTerminatingZero(v.c_str(), v.size());
        }
        else
            assert_cast<ColVecType &>(to).insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
    {
        /// Write size first
        writeVarInt(values.size(), buf);

        /// Then write value / count
        for (const auto & [val, count] : values)
        {
            writeBinary(val, buf);
            writeVarUInt(count, buf);
        }
    }

    void read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena *)
    {
        Int64 size = 0;
        readVarInt(size, buf);

        values.setCapacity(std::max(size, values.capacity()));

        for (Int64 i = 0; i < size; ++i)
        {
            String value;
            UInt32 count;

            readBinary(value, buf);
            readVarUInt(count, buf);

            [[maybe_unused]] auto inserted = values.insert(std::move(value), count);
            assert(inserted);
        }
    }

    bool add(const IColumn & column, size_t row_num, Arena *)
    {
        return values.insert(assert_cast<const ColVecType &>(column).getDataAtWithTerminatingZero(row_num).toString());
    }

    bool negate(const IColumn & column, size_t row_num, Arena *)
    {
        return values.erase(assert_cast<const ColVecType &>(column).getDataAtWithTerminatingZero(row_num).toString());
    }

    bool merge(const Self & rhs, Arena *)
    {
        values.merge(rhs.values);
        return true;
    }

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

template <bool maximum>
struct CountedValuesDataGeneric
{
private:
    using Self = CountedValuesDataGeneric;

    CountedValueMap<Field, maximum> values;

public:
    CountedValuesDataGeneric() : CountedValuesDataGeneric(1) { }
    explicit CountedValuesDataGeneric(int64_t size) : values(size) { }

    static constexpr bool is_nullable = false;

    bool has() const { return !values.empty(); }

    void insertResultInto(IColumn & to) const
    {
        if (has())
            to.insert(values.firstValue());
        else
            to.insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        /// Write size first
        writeVarInt(values.size(), buf);

        /// Then write value / count
        for (const auto & [val, count] : values)
        {
            serialization.serializeBinary(val, buf);
            writeVarUInt(count, buf);
        }
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena *)
    {
        Int64 size = 0;
        readVarInt(size, buf);

        values.setCapacity(std::max(size, values.capacity()));

        for (Int64 i = 0; i < size; ++i)
        {
            Field value;
            UInt32 count;

            serialization.deserializeBinary(value, buf);
            readVarUInt(count, buf);

            [[maybe_unused]] auto inserted = values.insert(std::move(value), count);
            assert(inserted);
        }
    }

    bool add(const IColumn & column, size_t row_num, Arena *) { return values.insert(column[row_num]); }

    bool negate(const IColumn & column, size_t row_num, Arena *) { return values.erase(column[row_num]); }

    bool merge(const Self & rhs, Arena *)
    {
        values.merge(rhs.values);
        return true;
    }

    static bool allocatesMemoryInArena() { return false; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

/** What is the difference between the aggregate functions min, max, any, any_last
  *  (the condition that the stored value is replaced by a new one,
  *   as well as, of course, the name).
  */

template <typename Data>
struct AggregateFunctionMinData : Data
{
    using Self = AggregateFunctionMinData;

    using Data::Data;

    static const char * name() { return "min"; }

#if 0
#    if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeIfLess(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeIfLessMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#    endif
#endif
};

template <typename Data>
struct AggregateFunctionMaxData : Data
{
    using Self = AggregateFunctionMaxData;

    using Data::Data;

    static const char * name() { return "max"; }

#if 0
#    if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeIfGreater(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeIfGreaterMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#    endif
#endif
};

template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeFirstTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeFirstTime(to, arena); }

    static const char * name() { return "any"; }

#if 0
#    if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeFirstTime(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeFirstTimeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#    endif
#endif
};

template <typename Data>
struct AggregateFunctionAnyLastData : Data
{
    using Self = AggregateFunctionAnyLastData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena) { return this->changeEveryTime(column, row_num, arena); }
    bool changeIfBetter(const Self & to, Arena * arena) { return this->changeEveryTime(to, arena); }

    static const char * name() { return "any_last"; }

#if 0
#    if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = Data::is_compilable;

    static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
    {
        Data::compileChangeEveryTime(builder, aggregate_data_ptr, value_to_check);
    }

    static void
    compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
    {
        Data::compileChangeEveryTimeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

#    endif
#endif
};

template <typename Data>
struct AggregateFunctionSingleValueOrNullData : Data
{
    static constexpr bool is_nullable = true;

    using Self = AggregateFunctionSingleValueOrNullData;

    bool first_value = true;
    bool is_null = false;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            this->change(column, row_num, arena);
            return true;
        }
        else if (!this->isEqualTo(column, row_num))
        {
            is_null = true;
        }
        return false;
    }

    bool changeIfBetter(const Self & to, Arena * arena)
    {
        if (first_value)
        {
            first_value = false;
            this->change(to, arena);
            return true;
        }
        else if (!this->isEqualTo(to))
        {
            is_null = true;
        }
        return false;
    }

    void insertResultInto(IColumn & to) const
    {
        if (is_null || first_value)
        {
            to.insertDefault();
        }
        else
        {
            ColumnNullable & col = typeid_cast<ColumnNullable &>(to);
            col.getNullMapColumn().insertDefault();
            this->Data::insertResultInto(col.getNestedColumn());
        }
    }

    static const char * name() { return "single_value_or_null"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

/** Implement 'heavy hitters' algorithm.
  * Selects most frequent value if its frequency is more than 50% in each thread of execution.
  * Otherwise, selects some arbitrary value.
  * http://www.cs.umd.edu/~samir/498/karp.pdf
  */
template <typename Data>
struct AggregateFunctionAnyHeavyData : Data
{
    UInt64 counter = 0;

    using Self = AggregateFunctionAnyHeavyData;

    bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)
    {
        if (this->isEqualTo(column, row_num))
        {
            ++counter;
        }
        else
        {
            if (counter == 0)
            {
                this->change(column, row_num, arena);
                ++counter;
                return true;
            }
            else
                --counter;
        }
        return false;
    }

    bool changeIfBetter(const Self & to, Arena * arena)
    {
        if (this->isEqualTo(to))
        {
            counter += to.counter;
        }
        else
        {
            if ((!this->has() && to.has()) || counter < to.counter)
            {
                this->change(to, arena);
                return true;
            }
            else
                counter -= to.counter;
        }
        return false;
    }

    void write(WriteBuffer & buf, const ISerialization & serialization) const
    {
        Data::write(buf, serialization);
        writeBinary(counter, buf);
    }

    void read(ReadBuffer & buf, const ISerialization & serialization, Arena * arena)
    {
        Data::read(buf, serialization, arena);
        readBinary(counter, buf);
    }

    static const char * name() { return "any_heavy"; }

#if USE_EMBEDDED_COMPILER

    static constexpr bool is_compilable = false;

#endif
};

/// In changelog mode, when we only keep around `retract_max` elements in CountedArgValueMap, there are situations
/// that we won't get the correct results. Examples retract_max = 3
/// For max sequence (value, delta): (10, 1), (9, 1), (8, 1), (7, 1), (6, 1), (10, -1), (9, -1), (8, -1), (5, 1)
/// (7, 1) and (6, 1) will be dropped on the floor since we reach the capacity N = 3 when they appear
/// Then, 10, 9, 8 get retracted, the map is basically empty at this point of time
/// And then (5, 1) gets inserted and it is the only element in the map, so it is the max element at
/// this specific time but it is wrong since we dropped 7 and 6.
/// On the other hand, if we keep all unique values and their counts in the map (retract_max == 0), we can always get the correct result
/// so setting up a correct retract_max is a trade-off between an absolute accurate result and resource usage
template <typename Data>
class AggregateFunctionsCountedValue final : public IAggregateFunctionDataHelper<Data, AggregateFunctionsCountedValue<Data>>
{
private:
    SerializationPtr serialization;
    UInt64 max_size;

public:
    explicit AggregateFunctionsCountedValue(const DataTypePtr & type, const Settings * settings)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionsCountedValue<Data>>({type}, {})
        , serialization(type->getDefaultSerialization())
        , max_size(settings->retract_max.value)
    {
        if (StringRef(Data::name()) == StringRef("min") || StringRef(Data::name()) == StringRef("max"))
        {
            if (!type->isComparable())
                throw Exception(
                    "Illegal type " + type->getName() + " of argument of aggregate function " + getName()
                        + " because the values of that data type are not comparable",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    void create(AggregateDataPtr place) const override { new (place) Data(max_size); }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        auto result_type = this->argument_types.at(0);
        if constexpr (Data::is_nullable)
            return makeNullable(result_type);
        return result_type;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(*columns[0], row_num, arena);
    }

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const final
    {
        this->data(place).negate(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }

#if 0
#    if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        if constexpr (!Data::is_compilable)
            return false;

        return canBeNativeType(*this->argument_types[0]);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        b.CreateMemSet(
            aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), this->sizeOfData(), llvm::assumeAligned(this->alignOfData()));
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes &,
        const std::vector<llvm::Value *> & argument_values) const override
    {
        if constexpr (Data::is_compilable)
        {
            Data::compileChangeIfBetter(builder, aggregate_data_ptr, argument_values[0]);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        if constexpr (Data::is_compilable)
        {
            Data::compileChangeIfBetterMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        if constexpr (Data::is_compilable)
        {
            return Data::compileGetResult(builder, aggregate_data_ptr);
        }
        else
        {
            throw Exception(getName() + " is not JIT-compilable", ErrorCodes::NOT_IMPLEMENTED);
        }
    }

#    endif
#endif
};
}
}
