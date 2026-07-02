#pragma once

#include <Storages/ExternalStream/Log/FileLogSource.h>

#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalStream/StorageExternalStreamImpl.h>

#include <filesystem>

namespace re2
{
class RE2;
}

namespace DB
{

class IStorage;

class FileLog final : public StorageExternalStreamImpl
{
public:
    FileLog(StorageID storage_id, StorageInMemoryMetadata metadata, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context);
    ~FileLog() override = default;

    void validate(const ContextPtr & context) const override;
    void validateSettings(const ExternalStreamSettingsPtr & new_settings, bool change_settings, const ContextPtr & context) const override;

    String getName() const override { return "FileLogExternalStream"; }

    bool supportsAccurateSeekTo() const noexcept override { return false; }
    bool supportsSubcolumns() const override { return false; }

    void startup() override { }
    void shutdown(bool /*dropping*/) override { }

    NamesAndTypesList getVirtuals() const override;

    std::optional<String> preferredColumn() const override;

    UInt64 hashBytes() const { return std::min<UInt64>(std::max<UInt64>(128, settings->hash_bytes.value), 2048); }

    const re2::RE2 & timestampRegex() const { return *timestamp_regex; }

    const re2::RE2 & linebreakerRegex() const { return *linebreaker_regex; }

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    FileLogSource::FileContainer searchForCandidates(bool table_query = false);

    bool isRemote() const override;

private:
    std::vector<std::unique_ptr<re2::RE2>> file_regexes;
    std::unique_ptr<re2::RE2> timestamp_regex;
    std::unique_ptr<re2::RE2> linebreaker_regex;

    Int64 start_timestamp = 0;
};
}
