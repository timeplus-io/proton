#pragma once

#include <Cluster/Common/MetaKeySpace.h>

#include <string>
#include <unordered_map>

namespace cluster::meta
{

enum class MetaCommand : uint8_t
{
    None = 0,
    Examine = 1,
    Delete = 2,
};

struct MetaArgs
{
    bool valid() const noexcept
    {
        if (dir.empty())
            return false;

        if (cmd == MetaCommand::None)
            return false;

        return true;
    }

    std::string dir;

    MetaCommand cmd = MetaCommand::None;
    /// If key space is std::nullopt, all key spaces
    std::optional<cluster::MetaKeySpace> key_space;
    std::string key_space_name;

    std::string ns;
    std::string key;
    std::string key2;

    /// If dump the meta object to stdout
    bool dump = false;
};


const std::unordered_map<std::string, cluster::MetaKeySpace> & keySpaces();

MetaArgs parseArgs(int argc, char ** argv);

}
