#include <Columns/ColumnString.h>
#include <Core/Field.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationString.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

Field DataTypeString::getDefault() const
{
    return String();
}

MutableColumnPtr DataTypeString::createColumn() const
{
    return ColumnString::create();
}


bool DataTypeString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this);
}

SerializationPtr DataTypeString::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationString>();
}

static DataTypePtr create(const ASTPtr & arguments/* proton: starts */, [[maybe_unused]] bool compatible_with_clickhouse = false/* proton: ends */)
{
    if (arguments && !arguments->children.empty())
    {
        if (arguments->children.size() > 1)
            throw Exception("The string data type family mustn't have more than one argument - size in characters", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * argument = arguments->children[0]->as<ASTLiteral>();
        if (!argument || argument->value.getType() != Field::Types::UInt64)
            throw Exception("The string data type family may have only a number (positive integer) as its argument", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
    }

    return std::make_shared<DataTypeString>();
}


void registerDataTypeString(DataTypeFactory & factory)
{
    factory.registerDataType("string", create);

    /// These synonims are added for compatibility.

    /// factory.registerAlias("CHAR", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NCHAR", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("CHARACTER", "string", DataTypeFactory::CaseInsensitive);
    factory.registerAlias("VARCHAR", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NVARCHAR", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("VARCHAR2", "string", DataTypeFactory::CaseInsensitive); /// Oracle
    /// factory.registerAlias("TEXT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("TINYTEXT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("MEDIUMTEXT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("LONGTEXT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BLOB", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("CLOB", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("TINYBLOB", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("MEDIUMBLOB", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("LONGBLOB", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BYTEA", "string", DataTypeFactory::CaseInsensitive); /// PostgreSQL

    /// factory.registerAlias("CHARACTER LARGE OBJECT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("CHARACTER VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("CHAR LARGE OBJECT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("CHAR VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NATIONAL CHAR", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NATIONAL CHARACTER", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NATIONAL CHARACTER LARGE OBJECT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NATIONAL CHARACTER VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NATIONAL CHAR VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NCHAR VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("NCHAR LARGE OBJECT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BINARY LARGE OBJECT", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("BINARY VARYING", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("VARBINARY", "string", DataTypeFactory::CaseInsensitive);
    /// factory.registerAlias("GEOMETRY", "string", DataTypeFactory::CaseInsensitive); //mysql

    /// proton: starts
    factory.registerClickHouseAlias("String", "string");
    /// proton: ends
}
}
