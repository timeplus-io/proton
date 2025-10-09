#pragma once

#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{

class ASTDropFunctionQuery : public ASTQueryWithOutput, public ASTQueryWithOnCluster
{
public:
    String function_name;

    bool if_exists = false;

    String getID(char) const override { return "DropFunctionQuery"; }

    ASTPtr clone() const override;

    void formatQueryImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropFunctionQuery>(clone()); }
};

}
