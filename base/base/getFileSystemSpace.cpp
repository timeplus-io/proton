#include <base/getFileSystemSpace.h>

#include <filesystem>


FileSystemSpace getFileSystemSpace(const std::string & path)
{
    try
    {
        std::filesystem::path p = path;
        auto space = std::filesystem::space(p);
        return {.capacity = space.capacity, .free = space.free, .available = space.available, .used = space.capacity - space.free};
    }
    catch (...)
    {
        /// if it cannot determine the size of file system, return 0.
    }
    return {.capacity = 0, .free = 0, .available = 0, .used = 0};
}
