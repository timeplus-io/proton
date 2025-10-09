#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
/// SHOW FUNCTIONS
class ASTShowFunctionsQuery : public ASTQueryWithOutput
{
public:
    ASTPtr where_expression;
    String getID(char) const override { return "ShowFunctionsQuery"; }
    ASTPtr clone() const override
    {
        auto clone = std::make_shared<ASTShowFunctionsQuery>(*this);
        if (where_expression)
        {
            clone->where_expression = where_expression->clone();
            clone->children.clear();
            clone->children.emplace_back(clone->where_expression);
        }
        return clone;
    }
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
