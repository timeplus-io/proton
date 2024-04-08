#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
}

void StorageExternalStreamImpl::createTempDirIfNotExists() const
{
    std::error_code err;
    /// create_directories will do nothing if the directory already exists.
    if (!fs::create_directories(tmpdir, err))
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "Failed to create external stream temproary directory, error_code={}, error_mesage={}", err.value(), err.message());
}

void StorageExternalStreamImpl::tryRemoveTempDir(Poco::Logger * logger) const
{
    LOG_INFO(logger, "Trying to remove external stream temproary directory {}", tmpdir.string());
    std::error_code err;
    fs::remove_all(tmpdir, err);
    if (err)
        LOG_ERROR(logger, "Failed to remove the temporary directory, error_code={}, error_message={}", err.value(), err.message());
}

}
