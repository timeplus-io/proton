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


void DataTypeArray::forEachChild(const ChildCallback & callback) const
{
    callback(*nested);
    nested->forEachChild(callback);
}

std::unique_ptr<ISerialization::SubstreamData> DataTypeArray::getDynamicSubcolumnData(std::string_view subcolumn_name, const DB::IDataType::SubstreamData & data, bool throw_if_null) const
{
    auto nested_type = assert_cast<const DataTypeArray &>(*data.type).nested;
    auto nested_data = std::make_unique<ISerialization::SubstreamData>(nested_type->getDefaultSerialization());
    nested_data->type = nested_type;
    nested_data->column = data.column ? assert_cast<const ColumnArray &>(*data.column).getDataPtr() : nullptr;

    auto nested_subcolumn_data = nested_type->getSubcolumnData(subcolumn_name, *nested_data, throw_if_null);
    if (!nested_subcolumn_data)
        return nullptr;

    auto creator = SerializationArray::SubcolumnCreator(data.column ? assert_cast<const ColumnArray &>(*data.column).getOffsetsPtr() : nullptr);
    auto res = std::make_unique<ISerialization::SubstreamData>();
    res->serialization = creator.create(nested_subcolumn_data->serialization, nested_subcolumn_data->type);
    res->type = creator.create(nested_subcolumn_data->type);
    if (data.column)
        res->column = creator.create(nested_subcolumn_data->column);

    return res;
}

static DataTypePtr create(const ASTPtr & arguments, bool compatible_with_clickhouse = false) /// proton: updated
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "array data type family must have exactly one argument - type of elements");

    return std::make_shared<DataTypeArray>(DataTypeFactory::instance().get(arguments->children[0]/* proton: starts */, compatible_with_clickhouse/* proton: ends */));
}


void registerDataTypeArray(DataTypeFactory & factory)
{
    factory.registerDataType("array", create);

    /// proton: starts
    factory.registerClickHouseAlias("Array", "array");
    /// proton: ends
}

}
