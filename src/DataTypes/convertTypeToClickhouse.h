#pragma once

#include <string.h>
#include <unordered_map>
#include <boost/lexical_cast.hpp>
#include <stack>
#include <iostream>

namespace DB
{
extern std::unordered_map<std::string, std::string> typeMap;

std::string convertTypeToUpper(const std::string & input);

}
