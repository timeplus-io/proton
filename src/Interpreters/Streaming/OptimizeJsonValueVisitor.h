#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class ASTSelectQuery;
class ASTFunction;
class ASTIdentifier;

/// Visits AST node to optimize the `json_value` that have same raw json string to `json_values`.
/// For examples:
/// json_value(raw, `$.a`) as `raw_a`, raw:b as `raw_b`, raw:a
///
/// (Optimized) ->
///
/// with json_values(raw, '$.a', '$.b') as `__json_values_raw`
/// __json_values_raw[1] as `raw_a`,
/// __json_values_raw[2] as `raw_b`,
/// `raw_a` as `raw:a`
///
/// Limitation:
/// 1) Only used for select query
/// 2) Only optimizing in the same level
class OptimizeJsonValueMatcher
{
public:
    using Visitor = InDepthNodeVisitor<OptimizeJsonValueMatcher, false, true>;

    struct PathInfo
    {
        size_t index = 0;
        std::vector<ASTPtr *> nodes; /// We store all `json_value` node pointer, which used for update themselves after collected @json_value_info.
                                     /// These pointers are safe because we don't support nested `json_value` function
                                     /// , such as json_value(json_value(raw, '$.obj'), '$.data') (Unsupported)
                                     /// So they are always leaf nodes.
    };
    
    struct Data
    {
        ASTSelectQuery * select;

        /// <json col name, <elem path, PathInfo > >
        std::unordered_map<String, std::unordered_map<String, PathInfo>> json_value_info;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, ASTPtr & child, Data & data);

private:
    /// Create `json_values(...)` into with-clause of select query
    static void finalizeJsonValues(Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using OptimizeJsonValueVisitor = OptimizeJsonValueMatcher::Visitor;

}
