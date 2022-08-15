#pragma once

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTColumnDeclaration.h>

#include <Poco/JSON/Parser.h>

namespace DB
{
namespace Streaming
{
void ColumnDeclarationToJSON(Poco::JSON::Object & column_mapping_json, const ASTColumnDeclaration & col_decl);
String JSONToString(const Poco::JSON::Object & json);
}
}
