#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/Serializations/SerializationNothing.h>
#include <DataTypes/DataTypeFactory.h>
#include <Columns/ColumnNothing.h>


namespace DB
{

MutableColumnPtr DataTypeNothing::createColumn() const
{
    return ColumnNothing::create(0);
}

bool DataTypeNothing::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeNothing::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationNothing>();
}


void registerDataTypeNothing(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("nothing", [] { return DataTypePtr(std::make_shared<DataTypeNothing>()); });

    /// proton: starts
    factory.registerClickHouseAlias("Nothing", "nothing");
    /// proton: ends
}

}
