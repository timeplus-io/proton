#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
struct StreamingWindowDescription
{
    StreamingWindowDescription(
        const std::shared_ptr<ASTFunction> & func_node_, const Names & argument_names_, const DataTypes & argument_types_)
        : func_node(func_node_), argument_names(argument_names_), argument_types(argument_types_)
    {
    }

    std::shared_ptr<ASTFunction> func_node;
    Names argument_names;
    DataTypes argument_types;
};

}
