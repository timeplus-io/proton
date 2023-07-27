#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Streaming
{
struct TimestampFunctionDescription
{
    TimestampFunctionDescription(ASTPtr func_ast_, ExpressionActionsPtr expr_, Names input_columns_, bool is_now)
        : func_ast(std::move(func_ast_)), expr(std::move(expr_)), input_columns(std::move(input_columns_)), is_now_func(is_now)
    {
        assert(func_ast);
    }

    ASTPtr func_ast;

    ExpressionActionsPtr expr;

    Names input_columns;

    bool is_now_func;
};

}
}
