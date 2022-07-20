#include "DataTypeBool.h"

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationBool.h>

namespace DB
{

size_t DataTypeBool::getSizeOfValueInMemory() const
{
    return sizeof(ColumnBool::ValueType);
}

SerializationPtr DataTypeBool::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationBool>(std::make_shared<SerializationNumber<Bool>>());
}

void registerDataTypeBool(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("bool", [] { return std::make_shared<DataTypeBool>(); });
    factory.registerAlias("boolean", "bool", DataTypeFactory::CaseSensitive);
}

}
