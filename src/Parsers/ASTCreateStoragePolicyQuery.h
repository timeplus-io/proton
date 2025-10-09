#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTCreateStoragePolicyQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String name;
    String yaml_config;

    bool or_replace = false;
    bool if_not_exists = false;

    String getID(char delim) const override { return "CreateStoragePolicyQuery" + (delim + name); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateStoragePolicyQuery>(clone()); }
};

}
