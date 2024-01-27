#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationIPv4andIPv6.h>


namespace DB
{

void registerDataTypeIPv4andIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("ipv4", [] { return DataTypePtr(std::make_shared<DataTypeIPv4>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerAlias("inet4", "ipv4", DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("ipv6", [] { return DataTypePtr(std::make_shared<DataTypeIPv6>()); }, DataTypeFactory::CaseInsensitive);
    factory.registerAlias("inet6", "ipv6", DataTypeFactory::CaseInsensitive);

    factory.registerClickHouseAlias("IPv4", "ipv4");
    factory.registerClickHouseAlias("INET4", "inet4");
    factory.registerClickHouseAlias("IPv6", "ipv6");
    factory.registerClickHouseAlias("INET6", "inet6");
}

}
