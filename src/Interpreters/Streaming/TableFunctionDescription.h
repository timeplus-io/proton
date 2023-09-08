#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/IAST_fwd.h>

#include <any>

namespace DB
{
namespace Streaming
{
struct TableFunctionDescription
{
    TableFunctionDescription(
        ASTPtr func_ast_,
        WindowType type_,
        Names argument_names_,
        DataTypes argument_types_,
        ExpressionActionsPtr args_expr_,
        Names input_columns_,
        NamesAndTypesList additional_result_columns_)
        : func_ast(std::move(func_ast_))
        , type(type_)
        , argument_names(std::move(argument_names_))
        , argument_types(std::move(argument_types_))
        , expr_before_table_function(std::move(args_expr_))
        , input_columns(std::move(input_columns_))
        , additional_result_columns(std::move(additional_result_columns_))
    {
        assert(func_ast);
    }

    ASTPtr func_ast;
    WindowType type;
    Names argument_names;
    DataTypes argument_types;

    std::any func_ctx;

    ExpressionActionsPtr expr_before_table_function;

    Names input_columns;
    NamesAndTypesList additional_result_columns;
};

}
}
