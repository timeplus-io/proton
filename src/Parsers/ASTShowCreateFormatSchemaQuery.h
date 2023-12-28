#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTShowCreateFormatSchemaQuery : public ASTQueryWithOutput
{
public:
    ASTPtr schema_name;
    String schema_type;

    String getSchemaName() const;

    String getID(char /*delim*/) const override { return "SHOW CREATE FORMAT SCHEMA " + getSchemaName(); }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTShowCreateFormatSchemaQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        res->schema_name = this->schema_name->clone();
        res->children.push_back(res->schema_name);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
