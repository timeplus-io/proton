#pragma once

#include "WindowCommon.h"

#include <Core/Names.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Streaming
{
struct FunctionDescription
{
    FunctionDescription(
        ASTPtr func_ast_,
        WindowType type_,
        Names argument_names_,
        DataTypes argument_types_,
        ExpressionActionsPtr expr_,
        ExpressionActionsPtr session_start_,
        ExpressionActionsPtr session_end_,
        Names input_columns_,
        ColumnNumbers keys_ = {},
        bool is_now = false)
        : func_ast(std::move(func_ast_))
        , type(type_)
        , argument_names(std::move(argument_names_))
        , argument_types(std::move(argument_types_))
        , expr(std::move(expr_))
        , session_start(session_start_)
        , session_end(session_end_)
        , input_columns(std::move(input_columns_))
        , keys(std::move(keys_))
        , is_now_func(is_now)
    {
        assert(func_ast);
    }

    ASTPtr func_ast;
    WindowType type;
    Names argument_names;
    DataTypes argument_types;

    ExpressionActionsPtr expr;

    ExpressionActionsPtr session_start;
    ExpressionActionsPtr session_end;

    Names input_columns;

    /// positions of key columns
    const ColumnNumbers keys;

    bool is_now_func = false;
};

using FunctionDescriptionPtr = std::shared_ptr<FunctionDescription>;
using FunctionDescriptionPtrs = std::vector<FunctionDescriptionPtr>;

}
}
