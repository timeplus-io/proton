#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
/// SHOW FORMAT SCHEMAS [TYPE schema_type]
class ASTShowFormatSchemasQuery : public ASTQueryWithOutput
{
public:
    String schema_type;

    String getID(char) const override { return "ShowFormatSchemasQuery"; }
    ASTPtr clone() const override { return std::make_shared<ASTShowFormatSchemasQuery>(*this); }
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
