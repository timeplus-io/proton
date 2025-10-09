#include <gtest/gtest.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/DataTypeFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED_METHOD;
}


void check(const DataTypePtr & type)
{
//    std::cerr << "Check " << type->getName() << "\n";
    WriteBufferFromOwnString ostr;
    encodeDataType(type, ostr);
    ReadBufferFromString istr(ostr.str());
    DataTypePtr decoded_type = decodeDataType(istr);
    ASSERT_TRUE(istr.eof());
    ASSERT_EQ(type->getName(), decoded_type->getName());
    ASSERT_TRUE(type->equals(*decoded_type));
}

GTEST_TEST(DataTypesBinaryEncoding, EncodeAndDecode)
{
    tryRegisterAggregateFunctions();
    check(std::make_shared<DataTypeNothing>());
    check(std::make_shared<DataTypeInt8>());
    check(std::make_shared<DataTypeUInt8>());
    check(std::make_shared<DataTypeInt16>());
    check(std::make_shared<DataTypeUInt16>());
    check(std::make_shared<DataTypeInt32>());
    check(std::make_shared<DataTypeUInt32>());
    check(std::make_shared<DataTypeInt64>());
    check(std::make_shared<DataTypeUInt64>());
    check(std::make_shared<DataTypeInt128>());
    check(std::make_shared<DataTypeUInt128>());
    check(std::make_shared<DataTypeInt256>());
    check(std::make_shared<DataTypeUInt256>());
    check(std::make_shared<DataTypeFloat32>());
    check(std::make_shared<DataTypeFloat64>());
    check(std::make_shared<DataTypeDate>());
    check(std::make_shared<DataTypeDate32>());
    check(std::make_shared<DataTypeDateTime>());
    check(std::make_shared<DataTypeDateTime>("EST"));
    check(std::make_shared<DataTypeDateTime>("CET"));
    check(std::make_shared<DataTypeDateTime64>(3));
    check(std::make_shared<DataTypeDateTime64>(3, "EST"));
    check(std::make_shared<DataTypeDateTime64>(3, "CET"));
    check(std::make_shared<DataTypeString>());
    check(std::make_shared<DataTypeFixedString>(10));
    check(DataTypeFactory::instance().get("enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = -128)"));
    check(DataTypeFactory::instance().get("enum16('a' = 1, 'b' = 2, 'c' = 3, 'd' = -1000)"));
    check(std::make_shared<DataTypeDecimal32>(3, 6));
    check(std::make_shared<DataTypeDecimal64>(3, 6));
    check(std::make_shared<DataTypeDecimal128>(3, 6));
    check(std::make_shared<DataTypeDecimal256>(3, 6));
    check(std::make_shared<DataTypeUUID>());
    check(DataTypeFactory::instance().get("array(uint32)"));
    check(DataTypeFactory::instance().get("array(array(array(uint32)))"));
    check(DataTypeFactory::instance().get("tuple(uint32, string, uuid)"));
    check(DataTypeFactory::instance().get("tuple(uint32, string, tuple(uuid, Date, IPv4))"));
    check(DataTypeFactory::instance().get("tuple(c1 uint32, c2 string, c3 uuid)"));
    check(DataTypeFactory::instance().get("tuple(c1 uint32, c2 string, c3 tuple(c4 uuid, c5 Date, c6 IPv4))"));
    check(std::make_shared<DataTypeSet>());
    check(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Nanosecond));
    check(std::make_shared<DataTypeInterval>(IntervalKind::Kind::Microsecond));
    check(DataTypeFactory::instance().get("nullable(uint32)"));
    check(DataTypeFactory::instance().get("nullable(nothing)"));
    check(DataTypeFactory::instance().get("nullable(uuid)"));
    check(std::make_shared<DataTypeFunction>(
        DataTypes{
            std::make_shared<DataTypeInt8>(),
            std::make_shared<DataTypeDate>(),
            DataTypeFactory::instance().get("array(array(array(uint32)))")},
        DataTypeFactory::instance().get("tuple(c1 uint32, c2 string, c3 uuid)")));
    DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
    Array parameters = {Field(0.1), Field(0.2)};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function = AggregateFunctionFactory::instance().get("quantiles", argument_types, parameters, properties);
    check(std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters));
    check(std::make_shared<DataTypeAggregateFunction>(function, argument_types, parameters, 2));
    check(DataTypeFactory::instance().get("aggregate_function(sum, uint64)"));
    check(DataTypeFactory::instance().get("aggregate_function(quantiles(0.5, 0.9), uint64)"));
    check(DataTypeFactory::instance().get("aggregate_function(sequence_match('(?1)(?2)'), Date, uint8, uint8)"));
    /// proton: starts
    /// TODO: fix below check having Syntax error
    /// check(DataTypeFactory::instance().get("aggregate_function(sum_map_filtered([1, 4, 8]), array(uint64), array(uint64))"));
    /// proton: ends
    check(DataTypeFactory::instance().get("low_cardinality(uint32)"));
    check(DataTypeFactory::instance().get("low_cardinality(nullable(string))"));
    check(DataTypeFactory::instance().get("map(string, uint32)"));
    check(DataTypeFactory::instance().get("map(string, map(string, map(string, uint32)))"));
    check(std::make_shared<DataTypeIPv4>());
    check(std::make_shared<DataTypeIPv6>());
    check(DataTypeFactory::instance().get("variant(string, uint32, date32)"));
    check(std::make_shared<DataTypeDynamic>());
    check(std::make_shared<DataTypeDynamic>(10));
    check(std::make_shared<DataTypeDynamic>(255));
    check(DataTypeFactory::instance().get("bool"));
    check(DataTypeFactory::instance().get("simple_aggregate_function(sum, uint64)"));
    check(DataTypeFactory::instance().get("simple_aggregate_function(max_map, tuple(array(uint32), array(uint32)))"));
    check(DataTypeFactory::instance().get("simple_aggregate_function(group_array_array(19), array(uint64))"));
    check(DataTypeFactory::instance().get("nested(a uint32, b uint32)"));
    check(DataTypeFactory::instance().get("nested(a uint32, b nested(c string, d nested(e Date)))"));
    /// proton: starts
    /// check(DataTypeFactory::instance().get("Ring"));
    /// check(DataTypeFactory::instance().get("Point"));
    /// check(DataTypeFactory::instance().get("Polygon"));
    /// check(DataTypeFactory::instance().get("MultiPolygon"));
    /// proton: ends
    check(DataTypeFactory::instance().get("tuple(map(low_cardinality(string), array(aggregate_function(2, quantiles(0.1, 0.2), float32))), array(array(tuple(uint32, tuple(a map(string, string), b nullable(Date), c variant(tuple(g string, d array(uint32)), Date, map(string, string)))))))"));
    check(DataTypeFactory::instance().get("json"));
    check(DataTypeFactory::instance().get("json(max_dynamic_paths=10)"));
    check(DataTypeFactory::instance().get("json(max_dynamic_paths=10, max_dynamic_types=10, a.b.c uint32, SKIP a.c, b.g string, SKIP l.d.f)"));
}
