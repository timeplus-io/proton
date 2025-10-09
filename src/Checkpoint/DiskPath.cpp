#include <Checkpoint/DiskPath.h>

#include <Disks/DiskLocal.h>

namespace DB
{
bool copyDiskPathViaHardLinking(const DiskPath & from, const DiskPath & to)
{
    try
    {
        /// TODO: Better same disk check
        if (from.disk.get() == to.disk.get())
        {
            if (to.disk->exists(to.path))
                to.disk->removeRecursive(to.path);

            if (from.disk->isDirectory(from.path))
            {
                to.disk->createDirectories(to.path);

                auto base_path_size = from.path.string().size();
                iterateDirectory(
                    from.disk,
                    from.path,
                    [&](const fs::path & path, bool is_dir) {
                        /// from_path/xxx/yyy => to_path/xxx/yyy
                        auto to_path = to.path / path.string().substr(base_path_size);
                        if (is_dir)
                        {
                            to.disk->createDirectories(to_path);
                            return;
                        }

                        from.disk->createHardLink(path, to_path);
                    },
                    /*recursive=*/true);
            }
            else
            {
                from.disk->createHardLink(from.path, to.path);
            }
            return true;
        }
        else if (dynamic_pointer_cast<DiskLocal>(from.disk) && dynamic_pointer_cast<DiskLocal>(to.disk))
        {
            auto from_path = fs::path(from.disk->getPath()) / from.path;
            auto to_path = fs::path(to.disk->getPath()) / to.path;
            fs::copy(from_path, to_path, fs::copy_options::recursive | fs::copy_options::create_hard_links);
            return true;
        }
    }
    catch (...)
    {
        LOG_INFO(getLogger("copyDiskPathViaHardLinking"), "Failed to create hard link: {}", getCurrentExceptionMessage(false));
        to.disk->removeRecursive(to.path);
    }
    return false;
}

DiskPath::DiskPath(const std::string & local_fs_path)
{
    auto path_ = fs::path(local_fs_path);
    disk = std::make_shared<DiskLocal>("local_disk", path_.parent_path().string());
    path = path_.filename();
}

TemporaryDiskPath::~TemporaryDiskPath()
{
    try
    {
        disk->removeRecursive(path);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void iterateDirectory(
    const DiskPtr & disk,
    const std::string & dir,
    const std::function<void(const fs::path & /*path*/, bool /*is_dir*/)> & callback,
    bool recursive)
{
    if (!disk->isDirectory(dir))
        return;

    try
    {
        for (auto dir_entry = disk->iterateDirectory(dir); dir_entry->isValid(); dir_entry->next())
        {
            try
            {
                fs::path path(dir_entry->path());
                if (disk->isDirectory(path))
                {
                    callback(path, /*is_dir=*/true);
                    if (recursive)
                        iterateDirectory(disk, path, callback, recursive);
                }
                else
                {
                    callback(path, /*is_dir=*/false);
                }
            }
            catch (const fs::filesystem_error & e)
            {
                /// If the file or sub-directory was deleted during iteration, skip it and continue with the next entry.
                if (e.code() == std::errc::no_such_file_or_directory)
                    continue;

                throw;
            }
        }
    }
    catch (const fs::filesystem_error & e)
    {
        /// If the entire directory was deleted during iteration, exit gracefully.
        if (e.code() == std::errc::no_such_file_or_directory)
            return;

        throw;
    }
}

void clearDirectory(const DiskPtr & disk, const std::string & dir)
{
    if (!disk->isDirectory(dir))
        return;

    for (auto dir_entry = disk->iterateDirectory(dir); dir_entry->isValid(); dir_entry->next())
        disk->removeRecursive(dir_entry->path());
}

size_t copyDiskPath(const DiskPath & from, const DiskPath & to, bool try_hard_link_first)
{
    if (try_hard_link_first)
    {
        if (copyDiskPathViaHardLinking(from, to))
            return 0;
    }

    /// Fallback to real data copy
    if (from.disk->isDirectory(from.path))
    {
        from.disk->copyDirectoryContent(from.path, to.disk, to.path, /*read_settings=*/{}, /*write_settings=*/{});

        size_t total_size = 0;
        iterateDirectory(
            from.disk,
            from.path,
            [&](const fs::path & path, bool is_dir) {
                if (!is_dir)
                    return;

                total_size += from.disk->getFileSize(path);
            },
            /*recursive=*/true);
        return total_size;
    }
    else
    {
        from.disk->copyFile(from.path, *to.disk, to.path);
        return from.disk->getFileSize(from.path);
    }
}
}
