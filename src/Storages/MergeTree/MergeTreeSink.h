#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include "SequenceInfo.h"

#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class Block;
class StorageMergeTree;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

class MergeTreeSink final : public SinkToStorage
{
public:
    MergeTreeSink(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        size_t max_parts_per_block_,
        ContextPtr context_);

    ~MergeTreeSink() override;

    String getName() const override { return "MergeTreeSink"; }
    void consume(Chunk chunk) override;
    void onStart() override;

    /// proton: starts
    void setSequenceInfo(const SequenceInfoPtr & seq_info_) { seq_info = seq_info_; }
    void setMissingSequenceRanges(SequenceRanges missing_seq_ranges_) { missing_seq_ranges.swap(missing_seq_ranges_); }

private:
    bool ignorePartBlock(Int32 parts, Int32 part_index) const;
    /// proton: ends

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    ContextPtr context;
    StorageSnapshotPtr storage_snapshot;
    uint64_t chunk_dedup_seqnum = 0; /// input chunk ordinal number in case of dedup token

    /// proton: starts
    SequenceInfoPtr seq_info;
    SequenceRanges missing_seq_ranges;
    /// proton: ends
};

}
