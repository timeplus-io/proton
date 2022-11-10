#include "CheckpointStorageFactory.h"
#include "LocalFilesystemCheckpoint.h"

#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace
{
const std::string DEFAULT_CKPT_STORAGE_TYPE = "local_file_system";
const std::string LOCAL_FILE_SYSTEM_CKPT = "local_file_system";
const std::string DEFAULT_CKPT_DIR = "/var/lib/proton/checkpoint/";
const uint64_t DEFAULT_CKPT_INTERVAL = 900; /// in seconds
}

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

std::unique_ptr<CheckpointStorage> CheckpointStorageFactory::create(const Poco::Util::AbstractConfiguration & config)
{
    auto ckpt_storage_type = config.getString("checkpoint.storage_type", DEFAULT_CKPT_STORAGE_TYPE);
    if (ckpt_storage_type == LOCAL_FILE_SYSTEM_CKPT || ckpt_storage_type.empty())
    {
        auto ckpt_path = config.getString("checkpoint.path", DEFAULT_CKPT_DIR);
        auto ckpt_interval = config.getUInt64("checkpoint.interval", DEFAULT_CKPT_INTERVAL);

        return std::make_unique<LocalFileSystemCheckpoint>(std::move(ckpt_path), ckpt_interval);
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown checkpoint storage type '{}'", ckpt_storage_type);
    }
}
}
