#pragma once

#include <boost/noncopyable.hpp>

#include <filesystem>
#include <memory>

namespace fs = std::filesystem;

namespace nlog
{
class FileLock final : public boost::noncopyable
{
public:
    explicit FileLock(const std::filesystem::path & file);
    ~FileLock();

    void lock();

    bool tryLock();

    void unlock();

private:
    int fd;
    fs::path file;
};

using FileLockPtr = std::shared_ptr<FileLock>;
}
