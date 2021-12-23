#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
struct StreamingFunctionDescription
{
    StreamingFunctionDescription(
        ASTPtr func_ast_, Names argument_names_, DataTypes argument_types_, ExpressionActionsPtr expr_, Names input_columns_, bool is_now = false)
        : func_ast(std::move(func_ast_))
        , argument_names(std::move(argument_names_))
        , argument_types(std::move(argument_types_))
        , expr(std::move(expr_))
        , input_columns(std::move(input_columns_))
        , is_now_func(is_now)
    {
        assert(func_ast);
    }

    ASTPtr func_ast;
    Names argument_names;
    DataTypes argument_types;

    ExpressionActionsPtr expr;
    Names input_columns;

    bool is_now_func = false;
};

using StreamingFunctionDescriptionPtr = std::shared_ptr<StreamingFunctionDescription>;

}
