#pragma once

#include <string>
#include <vector>

namespace DB
{
struct PathSize
{
    PathSize(std::string path_, uint64_t size_) : path(std::move(path_)), size(size_) { }

    std::string path;
    uint64_t size; /// In bytes
};

using PathSizes = std::vector<PathSize>;

inline std::pair<size_t, std::vector<std::string>> sumOf(PathSizes && path_sizes)
{
    size_t total_bytes = 0;
    std::vector<std::string> paths;
    paths.reserve(path_sizes.size());

    for (auto & path_size : path_sizes)
    {
        total_bytes += path_size.size;
        paths.push_back(std::move(path_size.path));
    }

    return {total_bytes, std::move(paths)};
}
}
