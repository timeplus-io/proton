#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationObject.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/Operators.h>

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

String DataTypeObject::doGetName() const
{
    WriteBufferFromOwnString out;
    if (is_nullable)
        out << "object(nullable(" << quote << schema_format << "))";
    else
        out << "object(" << quote << schema_format << ")";
    return out.str();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The object data type family must have one argument - name of schema format");

    ASTPtr schema_argument = arguments->children[0];
    bool is_nullable = false;

    if (const auto * func = schema_argument->as<ASTFunction>())
    {
        if (func->name != "nullable" || func->arguments->children.size() != 1)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                "Expected 'nullable(<schema_name>)' as parameter for type object", func->name);

        schema_argument = func->arguments->children[0];
        is_nullable = true;
    }

    const auto * literal = schema_argument->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
            "The object data type family must have a const string as its schema name parameter");

    return std::make_shared<DataTypeObject>(literal->value.get<const String &>(), is_nullable);
}

void registerDataTypeObject(DataTypeFactory & factory)
{
    factory.registerDataType("object", create);
    factory.registerSimpleDataType("json",
        [] { return std::make_shared<DataTypeObject>("json", false); },
        DataTypeFactory::CaseInsensitive);
    factory.registerSimpleDataType("nullable_json",
        [] { return std::make_shared<DataTypeObject>("json", true); },
        DataTypeFactory::CaseInsensitive);
}

}
