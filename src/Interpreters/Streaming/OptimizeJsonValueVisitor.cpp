#include "OptimizeJsonValueVisitor.h"

#include <Common/ProtonCommon.h>
#include <Functions/JSONPath/Generator/GeneratorJSONPath.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    /// We assume that there is no conflict with an existing name here
    String getJsonValuesCommonAlias(const String & json_col_name) { return ProtonConsts::JSON_VALUES_PREFIX + json_col_name; }

    ASTPtr createReferenceNode(const String & referenced_name, const String & origin_name)
    {
        auto ref_node = std::make_shared<ASTIdentifier>(referenced_name);
        if (referenced_name != origin_name)
            ref_node->setAlias(origin_name);
        return ref_node;
    }
}

bool OptimizeJsonValueMatcher::needChildVisit(ASTPtr & node, ASTPtr &, Data & data)
{
    if (auto * select = node->as<ASTSelectQuery>())
        data.select = select;

    /// Don't descent into table functions and subqueries and special case for ArrayJoin.
    return !(node->as<ASTTableExpression>() || node->as<ASTSelectWithUnionQuery>() || node->as<ASTArrayJoin>());
}

void OptimizeJsonValueMatcher::visit(ASTPtr & ast, Data & data)
{
    if (auto select = ast->as<ASTSelectQuery>())
    {
        finalizeJsonValues(data);
        return;
    }

    auto * func = ast->as<ASTFunction>();
    if (!func || func->name != "json_value")
        return;

    if (func->arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "'The json_value' function require 2 arguments: (json_str, elem_path)");

    /// We only handle json column
    auto json_col = func->arguments->children[0]->as<ASTIdentifier>();
    if (!json_col)
        return;

    auto elem_path = func->arguments->children[1]->as<ASTLiteral>();
    if (!elem_path)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument (elem_path) of 'json_value' must be constant string");

    const auto & json_col_name = json_col->name();
    const auto & path = elem_path->value.safeGet<String>();

    /// <json col name, <elem path, PathInfo> >
    auto json_iter = data.json_value_info.find(json_col_name);
    if (json_iter != data.json_value_info.end())
    {
        auto & elem_paths = json_iter->second;
        auto path_iter = elem_paths.find(path);
        if (path_iter != elem_paths.end())
            path_iter->second.nodes.emplace_back(&ast);
        else
            elem_paths.emplace(path, PathInfo{.index = elem_paths.size() + 1, .nodes = {&ast}});
    }
    else
    {
        data.json_value_info[json_col_name].emplace(path, PathInfo{.index = 1, .nodes = {&ast}});
        data.json_cols.emplace(json_col_name, json_col->ptr());
    }
}

void OptimizeJsonValueMatcher::finalizeJsonValues(Data & data)
{
    ASTs common_expr_list;
    for (const auto & json_paths : data.json_value_info)
    {
        if (json_paths.second.size() == 1)
        {
            /// Single path:
            /// 1) Get @current_path_first_node from the first `json_value`
            auto & [path, info] = *json_paths.second.begin();
            if (info.nodes.size() == 1)
                continue; /// single path and only one node, ignore it.

            auto & current_path_first_node = *info.nodes.front();
            auto current_path_first_node_alias = current_path_first_node->getAliasOrColumnName();
            current_path_first_node->setAlias(current_path_first_node_alias); /// here we use 'alias' instead of 'code_name'

            /// 2) Update other nodes refer to the first node `json_value`
            for (size_t i = 1; i < info.nodes.size(); ++i)
            {
                auto & node = *info.nodes[i];
                node = createReferenceNode(current_path_first_node_alias, node->getAliasOrColumnName());
            }
        }
        else
        {
            /// Multiple paths:
            /// 1) Create a common `json_values` as `@common_json_values_alias`
            auto json_values_func = std::make_shared<ASTFunction>();
            json_values_func->name = "json_values";
            json_values_func->arguments = std::make_shared<ASTExpressionList>();
            json_values_func->children.push_back(json_values_func->arguments);
            json_values_func->arguments->children.emplace_back(data.json_cols[json_paths.first]->clone());

            std::vector<std::pair<String, PathInfo>> paths(json_paths.second.begin(), json_paths.second.end());
            std::sort(paths.begin(), paths.end(), [](const auto & l, const auto & r) { return l.second.index < r.second.index; });
            for (const auto & path : paths)
                json_values_func->arguments->children.emplace_back(std::make_shared<ASTLiteral>(path.first));

            auto common_json_values_alias = getJsonValuesCommonAlias(json_paths.first);

            json_values_func->setAlias(common_json_values_alias);
            common_expr_list.emplace_back(std::move(json_values_func));

            /// 2) Update these nodes refer to the corresponding elem of `json_values` result (array)
            for (auto & [_, info] : paths)
            {
                auto & current_path_first_node = *info.nodes.front();
                auto array_elem_node = makeASTFunction(
                    "array_element", std::make_shared<ASTIdentifier>(common_json_values_alias), std::make_shared<ASTLiteral>(info.index));

                auto current_path_first_node_alias = current_path_first_node->getAliasOrColumnName();
                array_elem_node->setAlias(current_path_first_node_alias);
                current_path_first_node = array_elem_node;

                for (size_t i = 1; i < info.nodes.size(); ++i)
                {
                    auto & node = *info.nodes[i];
                    node = createReferenceNode(current_path_first_node_alias, node->getAliasOrColumnName());
                }
            }
        }
    }

    /// Add the common expressions into with-clause
    if (common_expr_list.empty())
        return;

    assert(data.select);
    if (auto with_ast = data.select->with())
    {
        with_ast->children.insert(with_ast->children.end(), common_expr_list.begin(), common_expr_list.end());
    }
    else
    {
        auto with_expr_list = std::make_shared<ASTExpressionList>();
        with_expr_list->children.swap(common_expr_list);
        data.select->setExpression(ASTSelectQuery::Expression::WITH, with_expr_list);
    }
}

}
