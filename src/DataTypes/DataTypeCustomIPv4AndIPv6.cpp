#include <DataTypes/Serializations/SerializationIP.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>

namespace DB
{

void registerDataTypeDomainIPv4AndIPv6(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("ipv4", []
    {
        auto type = DataTypeFactory::instance().get("uint32");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("ipv4"), std::make_unique<SerializationIPv4>(type->getDefaultSerialization())));
    });

    factory.registerSimpleDataTypeCustom("ipv6", []
    {
        auto type = DataTypeFactory::instance().get("fixed_string(16)");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
            std::make_unique<DataTypeCustomFixedName>("ipv6"), std::make_unique<SerializationIPv6>(type->getDefaultSerialization())));
    });

    /// MySQL, MariaDB
    factory.registerAlias("INET4", "ipv4", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("INET6", "ipv6", DataTypeFactory::CaseInsensitive);
}

}
