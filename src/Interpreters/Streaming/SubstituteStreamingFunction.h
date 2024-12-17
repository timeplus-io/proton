#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>

namespace DB
{
namespace Streaming
{
struct StreamingFunctionData
{
    using TypeToVisit = ASTFunction;

    StreamingFunctionData(bool streaming_, bool is_changelog_) : streaming(streaming_), is_changelog(is_changelog_) { }

    void visit(ASTFunction & func, ASTPtr);

    bool emit_version = false;

    static bool ignoreSubquery(const ASTPtr & /*node*/, const ASTPtr & child);

private:
    bool streaming;
    bool is_changelog;

    static std::unordered_map<String, String> func_map;
    static std::unordered_map<String, String> changelog_func_map;

    /// only streaming query can use these functions
    static std::set<String> streaming_only_func;

    /// check whether or not the function support 'retract' for changelog:
    /// the 1st return value: indicates whether or not it support retract
    /// the 2nd return value: the alias name of function should be rewritten in the query, empty if not support changelog
    std::optional<String> supportChangelog(const String & function_name);
};

using SubstituteStreamingFunctionVisitor
    = InDepthNodeVisitor<OneTypeMatcher<StreamingFunctionData, StreamingFunctionData::ignoreSubquery>, false>;


void substitueFunction(ASTFunction & func, const String & new_name);

struct SubstituteFunctionsData
{
    using TypeToVisit = ASTFunction;

    std::unordered_map<String, String> func_map;

    void visit(ASTFunction & func, ASTPtr &) const
    {
        auto iter = func_map.find(Poco::toLower(func.name));
        if (iter != func_map.end())
            substitueFunction(func, iter->second);
    }
};

using SubstituteFunctionsVisitor = InDepthNodeVisitor<OneTypeMatcher<SubstituteFunctionsData>, false>;
}
}