#include <DataTypes/ClickHouseDataTypeTranslator.h>

namespace DB
{

ClickHouseDataTypeTranslator & ClickHouseDataTypeTranslator::instance()
{
    static ClickHouseDataTypeTranslator ret;
    return ret;
}

ClickHouseDataTypeTranslator::ClickHouseDataTypeTranslator()
{
    /// referece: DataTypeFactory
    type_dict = {
        {"UInt8", "uint8"},
        {"UInt16", "uint16"},
        {"UInt32", "uint32"},
        {"UInt64", "uint64"},

        {"Int8", "int8"},
        {"Int16", "int16"},
        {"Int32", "int32"},
        {"Int64", "int64"},

        {"Float32", "float32"},
        {"Float64", "float64"},

        {"UInt128", "uint128"},
        {"UInt256", "uint256"},

        {"Int128", "int128"},
        {"Int256", "int256"},

        {"BYTE", "byte"},
        {"SMALLINT", "smallint"},
        {"INT", "int"},
        {"UINT", "uint"},
        {"INTEGER", "integer"},
        {"BIGINT", "bigint"},
        {"FLOAT", "float"},
        {"DOUBLE", "double"},

        {"Decimal32", "decimal32"},
        {"Decimal64", "decimal64"},
        {"Decimal128", "decimal128"},
        {"Decimal256", "decimal256"},
        {"Decimal", "decimal"},

        {"Date", "date"},
        {"Date32", "date32"},

        {"DateTime", "datetime"},
        {"DateTime32", "datetime32"},
        {"DateTime64", "datetime64"},

        {"String", "string"},
        {"VARCHAR", "VARCHAR"},

        {"FixedString", "fixed_string"},

        {"Enum8", "enum8"},
        {"Enum16", "enum16"},
        {"Enum", "enum"},

        {"Array", "array"},

        {"Tuple", "tuple"},

        {"Nullable", "nullable"},

        {"Nothing", "nothing"},

        {"UUID", "uuid"},

        {"IPv4", "ipv4"},
        {"INET", "inet"},
        {"IPv6", "ipv6"},
        {"INET6", "inet6"},

        {"AggregateFunction", "aggregate_function"},

        {"Nested", "nested"},

        {"IntervalNanosecond", "interval_nanosecond"},
        {"IntervalMicrosecond", "interval_microsecond"},
        {"IntervalMillisecond", "interval_millisecond"},
        {"IntervalSecond", "interval_second"},
        {"IntervalMinute", "interval_minute"},
        {"IntervalHour", "interval_hour"},
        {"IntervalDay", "interval_day"},
        {"IntervalWeek", "interval_week"},
        {"IntervalMonth", "interval_month"},
        {"IntervalQuarter", "interval_quarter"},
        {"IntervalYear", "interval_year"},

        {"LowCardinality", "low_cardinality"},

        {"Bool", "bool"},

        {"SimpleAggregateFunction", "simple_aggregate_function"},

        {"Map", "map"},

        {"JSON", "json"},
    };
}

std::string ClickHouseDataTypeTranslator::translate(const std::string & type_name)
{
    auto it = type_dict.find(type_name);
    if (it == type_dict.end())
        return type_name;

    return it->second;
}

}
