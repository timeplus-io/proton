#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

class ASTDropFormatSchemaQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String schema_name;
    String schema_type;

    bool if_exists = false;

    String getID(char) const override { return "DropFormatSchemaQuery"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTDropFormatSchemaQuery>(clone()); }
};

}
