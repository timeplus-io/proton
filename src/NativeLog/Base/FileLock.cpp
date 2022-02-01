#include "FileLock.h"

#include <Common/Exception.h>

#include <cassert>
#include <unistd.h>
#include <sys/file.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_LOCK_FILE;
}
}

namespace nlog
{
FileLock::FileLock(const std::filesystem::path & file_)
    : file(file_)
{
    fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0666);
    if (fd < 0)
        DB::throwFromErrnoWithPath(fmt::format("Cannot open lock file {}", file.c_str()), file.string(), DB::ErrorCodes::CANNOT_OPEN_FILE);
}

FileLock::~FileLock()
{
    ::close(fd);
}

/// Would block indefinitely if other process already locked the file until the lock-held process releases the lock
void FileLock::lock()
{
    if (flock(fd, LOCK_EX) == -1)
        DB::throwFromErrnoWithPath(fmt::format("Cannot lock file {}", file.c_str()), file.string(), DB::ErrorCodes::CANNOT_LOCK_FILE);
}

/// @return true if hold the lock successfully, otherwise false
bool FileLock::tryLock()
{
    /// If file lock was already held by another process, flock will return with errno: EWOULDBLOCK
    return flock(fd, LOCK_EX | LOCK_NB) != -1;
}

void FileLock::unlock()
{
    assert(fd >= 0);

    flock(fd, LOCK_UN);
}
}
