#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTSetQuery.h>

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
        Null
    };
    const char * getLanguageName() const
    {
        switch (lang)
        {
            case Language::SQL:
                return "sql";
            case Language::JavaScript:
                return "javascript";
            case Language::Python:
                return "python";
            default:
                return "unknown";
        }
    }

    /// proton: starts
    bool is_aggregation = false;
    Language lang = Language::Null;
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

    ASTPtr settings () const { return udf_settings; }

    /// If it is a JavaScript UDF
    bool isJavaScript() const noexcept { return lang == Language::JavaScript; }
    /// If it is a Python UDF
    bool isPython() const noexcept { return lang == Language::Python; }
    /// If it is a SQL UDF
    bool isSQL() const noexcept { return lang == Language::SQL; }
    /// proton: ends
};

}
