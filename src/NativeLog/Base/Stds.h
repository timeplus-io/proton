#pragma once

#include <filesystem>
#include <functional>
#include <string_view>

namespace fs = std::filesystem;

namespace std
{
template <>
struct hash<fs::path>
{
    size_t operator()(const fs::path & path) const noexcept { return hash<string_view>{}(path.c_str()); }
};
}

