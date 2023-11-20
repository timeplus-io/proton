#pragma once

#include "IParserBase.h"

namespace DB
{

/// CREATE FORMAT SCHEMA foo AS
/// $$
/// syntax = "proto3";
///
/// message SearchRequest {
///   string query = 1;
///   int32 page_number = 2;
///   int32 results_per_page = 3;
/// }
/// $$
/// TYPE Protobuf
class ParserCreateFormatSchemaQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE FORMAT SCHEMA query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
