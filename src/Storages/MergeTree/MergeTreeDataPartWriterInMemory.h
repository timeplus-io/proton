#pragma once
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>

namespace DB
{

/// Writes data part in memory.
class MergeTreeDataPartWriterInMemory : public IMergeTreeDataPartWriter
{
public:
    MergeTreeDataPartWriterInMemory(
        const MutableDataPartInMemoryPtr & part_,
        const String & data_part_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list_,
        const StorageMetadataPtr & metadata_snapshot_,
        const MergeTreeWriterSettings & settings_,
        const MergeTreeIndexGranularity & index_granularity_);

    /// You can write only one block. In-memory part can be written only at INSERT.
    void write(const Block & block, const IColumn::Permutation * permutation) override;

    void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) override;
    void finish(bool /*sync*/) override {}

private:
    void calculateAndSerializePrimaryIndex(const Block & primary_index_block);

    MutableDataPartInMemoryPtr part_in_memory;
};

}
