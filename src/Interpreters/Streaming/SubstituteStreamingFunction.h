#pragma once

#include <Parsers/ASTFunction.h>
#include <Interpreters/InDepthNodeVisitor.h>

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
};

using SubstituteStreamingFunctionVisitor = InDepthNodeVisitor<OneTypeMatcher<StreamingFunctionData, StreamingFunctionData::ignoreSubquery>, false>;

}
}