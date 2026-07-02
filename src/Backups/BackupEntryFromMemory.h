#pragma once

#include <Backups/IBackupEntry.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

/// Represents small preloaded data to be included in a backup.
class BackupEntryFromMemory : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromMemory(const void * data_, size_t size_, const std::optional<UInt128> & checksum_ = {});
    BackupEntryFromMemory(String data_, const std::optional<UInt128> & checksum_ = {});

    std::unique_ptr<ReadBuffer> getReadBuffer(const ReadSettings &) const override;
    UInt64 getSize() const override { return data.size(); }
    std::optional<UInt128> getChecksum() const override { return checksum; }

    String getFilePath() const override
    {
        return "";
    }

    DataSourceDescription getDataSourceDescription() const override
    {
        DataSourceDescription res;
        res.type = DataSourceType::RAM;
        return res;
    }

    DiskPtr tryGetDiskIfExists() const override { return nullptr; }

private:
    const String data;
    const std::optional<UInt128> checksum;
};

}
