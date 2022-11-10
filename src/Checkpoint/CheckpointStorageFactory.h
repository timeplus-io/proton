#pragma once

#include "CheckpointStorage.h"

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
struct CheckpointStorageFactory final
{
    static std::unique_ptr<CheckpointStorage> create(const Poco::Util::AbstractConfiguration & config);
};
}
