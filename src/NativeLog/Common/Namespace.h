#pragma once

#include <string>
#include <unordered_map>

namespace nlog
{
struct Namespace
{
    /// name is unique
    std::string name;
    std::string description;
    std::unordered_map<std::string, std::string> tags;
};
}