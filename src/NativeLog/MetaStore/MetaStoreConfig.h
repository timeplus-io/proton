#pragma once

#include <filesystem>

namespace fs = std::filesystem;

namespace nlog
{
struct MetaStoreConfig
{
    fs::path meta_dir;
};
}
