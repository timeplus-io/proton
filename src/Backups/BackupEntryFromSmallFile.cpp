#include <Backups/BackupEntryFromSmallFile.h>
#include <Disks/IDisk.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>


namespace DB
{
namespace
{
    String readFile(const String & file_path, const ReadSettings & read_settings)
    {
        auto buf = createReadBufferFromFileBase(file_path, read_settings);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }

    String readFile(const DiskPtr & disk, const String & file_path, const ReadSettings & read_settings)
    {
        auto buf = disk->readFile(file_path, read_settings);
        String s;
        readStringUntilEOF(s, *buf);
        return s;
    }
}


BackupEntryFromSmallFile::BackupEntryFromSmallFile(const String & file_path_, const ReadSettings & read_settings_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(readFile(file_path_, read_settings_), checksum_)
    , file_path(file_path_)
{
}

BackupEntryFromSmallFile::BackupEntryFromSmallFile(const DiskPtr & disk_, const String & file_path_, const ReadSettings & read_settings_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(readFile(disk_, file_path_, read_settings_), checksum_)
    , disk(disk_)
    , file_path(file_path_)
{
}

}
