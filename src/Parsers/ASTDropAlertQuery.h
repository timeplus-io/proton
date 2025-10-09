#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropAlertQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr database;
    ASTPtr name;

    bool if_exists = false;

    String getID(char) const override { return "DropAlertQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropAlertQuery>(clone()); }

    String getDatabase() const;
    String getName() const;
};

}
