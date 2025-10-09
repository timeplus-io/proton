#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropDiskQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String name;

    bool if_exists = false;

    String getID(char) const override { return "DropDiskQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropDiskQuery>(clone()); }
};

}
