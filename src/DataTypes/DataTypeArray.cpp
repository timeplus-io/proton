#include <Columns/ColumnArray.h>

#include <Formats/FormatSettings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationArray.h>

#include <Parsers/IAST.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Core/NamesAndTypes.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


DataTypeArray::DataTypeArray(const DataTypePtr & nested_)
    : nested{nested_}
{
}


MutableColumnPtr DataTypeArray::createColumn() const
{
    return ColumnArray::create(nested->createColumn(), ColumnArray::ColumnOffsets::create());
}


Field DataTypeArray::getDefault() const
{
    return Array();
}


bool DataTypeArray::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && nested->equals(*static_cast<const DataTypeArray &>(rhs).nested);
}

SerializationPtr DataTypeArray::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationArray>(nested->getDefaultSerialization());
}

size_t DataTypeArray::getNumberOfDimensions() const
{
    const DataTypeArray * nested_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!nested_array)
        return 1;
    return 1 + nested_array->getNumberOfDimensions();   /// Every modern C++ compiler optimizes tail recursion.
}


static DataTypePtr create(const ASTPtr & arguments/* proton: starts */, bool compatible_with_clickhouse = false/* proton: ends */)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception("array data type family must have exactly one argument - type of elements", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0], compatible_with_clickhouse));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("array", create);

    factory.registerClickHouseAlias("Array", "array");
}

}
