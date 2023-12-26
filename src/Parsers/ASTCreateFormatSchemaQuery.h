#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

namespace DB
{

class ASTCreateFormatSchemaQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr schema_name;
    ASTPtr schema_core;

    bool or_replace = false;
    bool if_not_exists = false;

    String schema_type;

    String getID(char delim) const override { return "CreateFormatSchemaQuery" + (delim + getSchemaName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateFormatSchemaQuery>(clone()); }

    String getSchemaName() const;

    String getSchemaBody() const;
};

}
