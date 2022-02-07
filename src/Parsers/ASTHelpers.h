#pragma once

#include <Parsers/ASTFunction.h>


namespace DB
{

static inline bool isFunctionCast(const ASTFunction * function)
{
    if (function)
        return function->name == "cast" || function->name == "_cast";
    return false;
}


}
