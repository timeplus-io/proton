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

    bool or_replace = false;
    bool if_not_exists = false;

    enum class Language
    {
        SQL,
        JavaScript,
        Python,
        Remote,
        Unknown
    };

    /// proton: starts
    bool is_aggregation = false;
    Language lang = Language::Unknown;
    ASTPtr arguments;
    ASTPtr return_type;
    ASTPtr udf_settings = nullptr;
    /// proton: ends

    String getID(char delim) const override { return "CreateFunctionQuery" + (delim + getFunctionName()); }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string &) const override { return removeOnCluster<ASTCreateFunctionQuery>(clone()); }

    String getFunctionName() const;

    /// proton: starts
    Poco::JSON::Object::Ptr toJSON() const;

    String getSource() const;
    bool isAggregation() const;
    String getReturnType() const;
    ASTPtr settings () const { return udf_settings; }

    bool isJavaScript() const noexcept { return lang == Language::JavaScript; }
    bool isPython() const noexcept { return lang == Language::Python; }
    bool isRemote() const noexcept { return lang == Language::Remote; }
    bool isUDF() const noexcept { return isJavaScript() || isPython() || isRemote(); }
    bool isSQL() const noexcept { return lang == Language::SQL; }
    /// proton: ends
};

}
