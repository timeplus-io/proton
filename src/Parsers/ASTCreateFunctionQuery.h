#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>

/// proton: starts
#include <Poco/JSON/Object.h>
/// proton: ends

namespace DB
{

class ASTCreateFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    ASTPtr function_name;
    ASTPtr function_core;
    Poco::JSON::Object::Ptr payload;

    bool or_replace = false;
    bool if_not_exists = false;

    /// proton: starts
    bool is_aggregation = false;
    String lang = "SQL";
    ASTPtr arguments;
    ASTPtr return_type;
    /// proton: ends

    String getID(char delim) const override { return "CreateFunctionQuery" + (delim + getFunctionName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateFunctionQuery>(clone()); }

    String getFunctionName() const;

    /// proton: starts
    Poco::JSON::Object::Ptr toJSON() const;

    /// If it is a JavaScript UDF
    bool isJavaScript() const noexcept { return lang == "JavaScript"; }

    /// If it is a JavaScript UDF
    bool isRemote() const noexcept{return lang == "remote";}
    /// proton: ends
};

}
