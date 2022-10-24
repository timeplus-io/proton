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
        bool start_with_boundary_,
        bool end_with_boundary_,
        Names input_columns_,
        bool is_now = false)
        : func_ast(std::move(func_ast_))
        , type(type_)
        , argument_names(std::move(argument_names_))
        , argument_types(std::move(argument_types_))
        , expr(std::move(expr_))
        , session_start(session_start_)
        , session_end(session_end_)
        , start_with_boundary(start_with_boundary_)
        , end_with_boundary(end_with_boundary_)
        , input_columns(std::move(input_columns_))
        , is_now_func(is_now)
    {
        assert(func_ast);
    }

    ASTPtr func_ast;
    WindowType type;
    Names argument_names;
    DataTypes argument_types;

    ExpressionActionsPtr expr;

    /// Only for session window
    /// Session start/end predication
    ExpressionActionsPtr session_start;
    ExpressionActionsPtr session_end;
    /// When the row matched session start/end predication
    /// If true, we keep this row in session window, otherwise not.
    bool start_with_boundary;
    bool end_with_boundary;

    Names input_columns;

    bool is_now_func = false;
};

using FunctionDescriptionPtr = std::shared_ptr<FunctionDescription>;
using FunctionDescriptionPtrs = std::vector<FunctionDescriptionPtr>;

}
}
