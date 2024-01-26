#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

bool DataTypeInterval::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && kind == static_cast<const DataTypeInterval &>(rhs).kind;
}


void registerDataTypeInterval(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("interval_nanosecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Nanosecond)); });
    factory.registerSimpleDataType("interval_microsecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Microsecond)); });
    factory.registerSimpleDataType("interval_millisecond", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Millisecond)); });
    factory.registerSimpleDataType("interval_second", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Second)); });
    factory.registerSimpleDataType("interval_minute", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Minute)); });
    factory.registerSimpleDataType("interval_hour", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Hour)); });
    factory.registerSimpleDataType("interval_day", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Day)); });
    factory.registerSimpleDataType("interval_week", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Week)); });
    factory.registerSimpleDataType("interval_month", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Month)); });
    factory.registerSimpleDataType("interval_quarter", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Quarter)); });
    factory.registerSimpleDataType("interval_year", [] { return DataTypePtr(std::make_shared<DataTypeInterval>(IntervalKind::Year)); });

    factory.registerClickHouseAlias("IntervalNanosecond", "interval_nanosecond");
    factory.registerClickHouseAlias("IntervalMicrosecond", "interval_microsecond");
    factory.registerClickHouseAlias("IntervalMillisecond", "interval_millisecond");
    factory.registerClickHouseAlias("IntervalSecond", "interval_second");
    factory.registerClickHouseAlias("IntervalMinute", "interval_minute");
    factory.registerClickHouseAlias("IntervalHour", "interval_hour");
    factory.registerClickHouseAlias("IntervalDay", "interval_day");
    factory.registerClickHouseAlias("IntervalWeek", "interval_week");
    factory.registerClickHouseAlias("IntervalMonth", "interval_month");
    factory.registerClickHouseAlias("IntervalQuarter", "interval_quarter");
    factory.registerClickHouseAlias("IntervalYear", "interval_year");
}

}
