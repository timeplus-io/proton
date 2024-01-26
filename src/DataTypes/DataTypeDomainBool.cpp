#include <DataTypes/Serializations/SerializationBool.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeCustom.h>

namespace DB
{

void registerDataTypeDomainBool(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("bool", []
    {
        auto type = DataTypeFactory::instance().get("uint8");
        return std::make_pair(type, std::make_unique<DataTypeCustomDesc>(
                std::make_unique<DataTypeCustomFixedName>("bool"), std::make_unique<SerializationBool>(type->getDefaultSerialization())));
    });

    factory.registerAlias("boolean", "bool");

    factory.registerClickHouseAlias("Bool", "bool");
}

}
