#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationObject.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/Operators.h>

/// proton: starts.
#include <DataTypes/Serializations/SerializationInfoObject.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

DataTypeObject::DataTypeObject(const String & schema_format_, bool is_nullable_)
    : schema_format(Poco::toLower(schema_format_))
    , is_nullable(is_nullable_)
    , default_serialization(getObjectSerialization(schema_format))
{
}

bool DataTypeObject::equals(const IDataType & rhs) const
{
    if (const auto * object = typeid_cast<const DataTypeObject *>(&rhs))
        return schema_format == object->schema_format && is_nullable == object->is_nullable;
    return false;
}

SerializationPtr DataTypeObject::doGetDefaultSerialization() const
{
    return default_serialization;
}

/// proton: starts.
SerializationPtr DataTypeObject::getSerialization(const SerializationInfo & info) const
{
    const auto & info_object = assert_cast<const SerializationInfoObject &>(info);
    return getObjectSerialization(schema_format, info_object.getPartialDeserializedSubcolumns());
}

MutableSerializationInfoPtr DataTypeObject::createSerializationInfo(const SerializationInfo::Settings & settings) const
{
    return std::make_shared<SerializationInfoObject>(ISerialization::Kind::DEFAULT, settings);
}
/// proton: ends.

String DataTypeObject::doGetName() const
{
    assert(schema_format == "json");
    return is_nullable ? "nullable_json" : "json";
}

void registerDataTypeObject(DataTypeFactory & factory)
{
    factory.registerSimpleDataType("json",
        [] { return std::make_shared<DataTypeObject>("json", false); },
        DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("nullable_json",
        [] { return std::make_shared<DataTypeObject>("json", true); },
        DataTypeFactory::CaseInsensitive);
}

}
