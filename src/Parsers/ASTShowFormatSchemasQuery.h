#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
/// SHOW FORMAT SCHEMAS [TYPE schema_type] [SETTINGS k=v]
class ASTShowFormatSchemasQuery : public ASTQueryWithOutput
{
public:
    String schema_type;

    String getID(char) const override { return "ShowFormatSchemasQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override;
};
}
