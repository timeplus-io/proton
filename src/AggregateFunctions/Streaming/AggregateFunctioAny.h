#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/StringRef.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Streaming/AggregateFunctionMinMax.h>
#include <AggregateFunctions/Streaming/CountedArgValueMap.h>

namespace DB
{
namespace streaming
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int LOGICAL_ERROR;
}


template <typename Data>
struct AggregateFunctionAnyData : Data
{
    using Self = AggregateFunctionAnyData;
    static constexpr bool is_any = true;
    

//     bool changeIfBetter(const IColumn & column, size_t row_num, Arena * arena)     { return this->changeFirstTime(column, row_num, arena); }
//     bool changeIfBetter(const Self & to, Arena * arena)                            { return this->changeFirstTime(to, arena); }
//     void addManyDefaults(const IColumn & column, size_t /*length*/, Arena * arena) { this->changeFirstTime(column, 0, arena); }

//     static const char * name() { return "any"; }

// #if USE_EMBEDDED_COMPILER

//     static constexpr bool is_compilable = Data::is_compilable;

//     static void compileChangeIfBetter(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
//     {
//         Data::compileChangeFirstTime(builder, aggregate_data_ptr, value_to_check);
//     }

//     static void compileChangeIfBetterMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
//     {
//         Data::compileChangeFirstTimeMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
//     }

// #endif
};



}
}
