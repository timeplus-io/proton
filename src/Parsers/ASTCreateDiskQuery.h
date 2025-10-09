#pragma once

#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTCreateDiskQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String name;
    ASTPtr disk_func;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateDiskQuery" + (delim + name); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateDiskQuery>(clone()); }
};

}
