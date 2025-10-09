#pragma once

#include <Disks/IDisk.h>

namespace DB
{
struct DiskPath
{
    DiskPtr disk;
    fs::path path;

    explicit DiskPath(const std::string & local_fs_path);
    DiskPath(DiskPtr disk_, std::string path_) : disk(std::move(disk_)), path(std::move(path_)) { }

    std::string logName() const { return fmt::format("{} on disk {}", path.string(), disk->logName()); }

    bool operator==(const DiskPath & other) const { return disk->getName() == other.disk->getName() && path == other.path; }
};

/// Automatically removes the directory/file when destroyed.
struct TemporaryDiskPath : public DiskPath
{
    using DiskPath::DiskPath;
    ~TemporaryDiskPath();
};

void iterateDirectory(
    const DiskPtr & disk,
    const std::string & dir,
    const std::function<void(const fs::path & /*path*/, bool /*is_dir*/)> & callback,
    bool recursive = false);

/// Remove all files/sub-directories from the directory. the directory itself is not removed.
void clearDirectory(const DiskPtr & disk, const std::string & dir);

/// \return the copied size in bytes (exclude hard links)
size_t copyDiskPath(const DiskPath & from, const DiskPath & to, bool try_hard_link_first);
/// \return true if hard link was created successfully, false if it failed (e.g., different disks)
bool copyDiskPathViaHardLinking(const DiskPath & from, const DiskPath & to);
}
