#pragma once

#include <Backups/IBackupEntry.h>


namespace DB
{

/// Concatenates data of two backup entries.
class BackupEntryConcat : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryConcat(
        BackupEntryPtr first_source_,
        BackupEntryPtr second_source_,
        const std::optional<UInt128> & checksum_ = {});

    UInt64 getSize() const override;
    std::optional<UInt128> getChecksum() const override { return checksum; }
    std::unique_ptr<ReadBuffer> getReadBuffer() const override;

    String getFilePath() const override { return ""; }

    DataSourceDescription getDataSourceDescription() const override { return {}; }

    DiskPtr tryGetDiskIfExists() const override { return nullptr; }

private:
    BackupEntryPtr first_source;
    BackupEntryPtr second_source;
    mutable std::optional<UInt64> size;
    std::optional<UInt128> checksum;
};

}
