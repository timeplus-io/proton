#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropTaskQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr database;
    ASTPtr name;

    bool if_exists = false;

    String getID(char) const override { return "DropTaskQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropTaskQuery>(clone()); }

    String getDatabase() const;
    String getName() const;
};

}
