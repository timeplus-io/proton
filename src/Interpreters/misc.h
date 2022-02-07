#pragma once

#include <Common/StringUtils/StringUtils.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

inline bool functionIsInOperator(const std::string & name)
{
    return name == "in" || name == "not_in" || name == "null_in" || name == "not_null_in";
}

inline bool functionIsInOrGlobalInOperator(const std::string & name)
{
    return functionIsInOperator(name) || name == "global_in" || name == "global_not_in" || name == "global_null_in" || name == "global_not_null_in";
}

inline bool functionIsLikeOperator(const std::string & name)
{
    return name == "like" || name == "ilike" || name == "not_like" || name == "not_ilike";
}

inline bool functionIsJoinGet(const std::string & name)
{
    return startsWith(name, "join_get");
}

inline bool functionIsDictGet(const std::string & name)
{
    return startsWith(name, "dict_get") || (name == "dict_has") || (name == "dict_is_in");
}

inline bool checkFunctionIsInOrGlobalInOperator(const ASTFunction & func)
{
    if (functionIsInOrGlobalInOperator(func.name))
    {
        size_t num_arguments = func.arguments->children.size();
        if (num_arguments != 2)
            throw Exception("Wrong number of arguments passed to function in. Expected: 2, passed: " + std::to_string(num_arguments),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        return true;
    }

    return false;
}

}
