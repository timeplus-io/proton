#pragma once

#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTColumnDeclaration.h>

#include <Poco/JSON/Parser.h>

namespace DB
{
namespace Streaming
{
void columnDeclarationToJSON(Poco::JSON::Object & column_mapping_json, const ASTColumnDeclaration & col_decl);
void settingsToJSON(Poco::JSON::Object & settings_json, const SettingsChanges & changes);
String jsonToString(const Poco::JSON::Object & json);
}
}
