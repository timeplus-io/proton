#pragma once

#include "Decimal.h"
#include "UUID.h"

namespace DB
{
struct Array;
struct Tuple;
struct Map;
struct AggregateFunctionStateData;

/**
 * Obtain type string representation from real type if possible.
 * @example TypeName<UInt8> == "UInt8"
 */
template <class T> constexpr inline std::string_view TypeName;

#define TN_MAP(_A, _T) \
    template <> constexpr inline std::string_view TypeName<_A> = #_T;

TN_MAP(UInt8, uint8)
TN_MAP(UInt16, uint16)
TN_MAP(UInt32, uint32)
TN_MAP(UInt64, uint64)
TN_MAP(UInt128, uint128)
TN_MAP(UInt256, uint256)
TN_MAP(Int8, int8)
TN_MAP(Int16, int16)
TN_MAP(Int32, int32)
TN_MAP(Int64, int64)
TN_MAP(Int128, int128)
TN_MAP(Int256, int256)
TN_MAP(Float32, float32)
TN_MAP(Float64, float64)
TN_MAP(String, string)
TN_MAP(UUID, uuid)
TN_MAP(Decimal32, decimal32)
TN_MAP(Decimal64, decimal64)
TN_MAP(Decimal128, decimal128)
TN_MAP(Decimal256, decimal256)
TN_MAP(DateTime64, datetime64)
TN_MAP(Array, array)
TN_MAP(Tuple, tuple)
TN_MAP(Map, map)

/// Special case
template <> constexpr inline std::string_view TypeName<AggregateFunctionStateData> = "AggregateFunctionState";

#undef TN_MAP
}
