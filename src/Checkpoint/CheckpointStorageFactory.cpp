#include <Checkpoint/CheckpointStorageFactory.h>
#include <Checkpoint/LocalFileSystemCheckpointStorage.h>

#include <Bootstrap/Globals.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <magic_enum.hpp>


namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INVALID_CONFIG_PARAMETER;
}

std::unique_ptr<CheckpointStorage> CheckpointStorageFactory::create(CheckpointReplicationType type, const CheckpointConfig & config)
{
    /// Only support LocalFileSystem storage
    static auto logger = getLogger("CheckpointStorageFactory");
    LOG_INFO(logger, "Creating checkpoint storage: type={} (forcing LocalFileSystem), path={}", magic_enum::enum_name(type), config.path);
    return std::make_unique<LocalFileSystemCheckpointStorage>(config.path);
}
}
