#pragma once

#include <string>

/// Represents the space info of file system in bytes.
/// "capacity" refers to the total size of the file system.
///
/// "free space" refers to the amount of space that is not currently being used by any files or system data.
///
/// "available space" refers to the amount of space that is available for use by unprivileged users.
/// In many file systems, a certain amount of space is reserved for system use and privileged processes (those running as root or another superuser).
/// This reserved space is not available to unprivileged users, even though it is technically free (i.e., not occupied by files).
/// So, the "available space" is typically less than the "free space".
/// It's the amount of free space minus the space reserved for the system.
/// This ensures that unprivileged users can't fill up the entire file system,
/// which could cause system processes to fail if they can't write to the disk.
///
/// "used space" refers to the amount of space that is currently being used by files or system data.
struct FileSystemSpace
{
    uint64_t capacity;
    uint64_t free;
    uint64_t available;
    uint64_t used;
};

/** Returns the space info of file system in bytes.
 * Returns 0 on unsupported platform or if it cannot determine the size of file system.
*/
FileSystemSpace getFileSystemSpace(const std::string & path);
