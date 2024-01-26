#include <DataTypes/DataTypeDate.h>
#include <DataTypes/Serializations/SerializationDate.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

bool DataTypeDate::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeDate::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationDate>();
}

void registerDataTypeDate(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("date", [] { return DataTypePtr(std::make_shared<DataTypeDate>()); }, DataTypeFactory::CaseInsensitive);

    // factory.registerClickHouseAlias("Date", "date");
}

}
