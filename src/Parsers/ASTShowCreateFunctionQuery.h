#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{

class ASTShowCreateFunctionQuery : public ASTQueryWithOutput
{
public:
    ASTPtr function_name;
    String getFunctionName() const;
    String getID(char /*delim*/) const override { return "SHOW CREATE FUNCTION " + getFunctionName(); }
    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTShowCreateFunctionQuery>(*this);
        res->children.clear();
        cloneOutputOptions(*res);
        res->function_name = this->function_name->clone();
        res->children.push_back(res->function_name);
        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
