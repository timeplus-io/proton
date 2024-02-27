#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <DataTypes/ObjectUtils.h>
#include <Common/logger_useful.h>

/// proton : starts
#include <Storages/MergeTree/SequenceInfo.h>
/// proton : ends

namespace ProfileEvents
{
    /// extern const Event DuplicatedInsertedBlocks;
}

namespace DB
{

struct MergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UInt64 elapsed_ns;
        String block_dedup_token;
        ProfileEvents::Counters part_counters;
    };

    std::vector<Partition> partitions;
};


MergeTreeSink::~MergeTreeSink() = default;

MergeTreeSink::MergeTreeSink(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock(), ProcessorID::MergeTreeSinkID)
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshot(metadata_snapshot, context))
{
}

void MergeTreeSink::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(nullptr, context, true);
}

void MergeTreeSink::onFinish()
{
    finishDelayedChunk();
}

void MergeTreeSink::consume(Chunk chunk)
{
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(nullptr, context, false);

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    if (!storage_snapshot->object_columns.get()->empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    /// proton: starts
    Int32 parts = static_cast<Int32>(part_blocks.size());
    Int32 part_index = 0;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        String block_dedup_token;

        if (ignorePartBlock(parts, part_index))
        {
            part_index++;
            continue;
        }

        SequenceInfoPtr part_seq;
        if (seq_info)
            part_seq = seq_info->shallowClone(part_index, parts);

        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, part_seq, context);

        part_index++;
        /// proton: ends

        UInt64 elapsed_ns = watch.elapsed();

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        if (!support_parallel_write && temp_part.part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        if (storage.getDeduplicationLog())
        {
            const String & dedup_token = settings.insert_deduplication_token;
            if (!dedup_token.empty())
            {
                /// multiple blocks can be inserted within the same insert query
                /// an ordinal number is added to dedup token to generate a distinctive block id for each block
                block_dedup_token = fmt::format("{}_{}", dedup_token, chunk_dedup_seqnum);
                ++chunk_dedup_seqnum;
            }
        }

        size_t max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        if (!support_parallel_write || settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;

        /// In case of too much columns/parts in block, flush explicitly.
        streams += temp_part.streams.size();
        if (streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk();
            delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk();

            streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        partitions.emplace_back(MergeTreeSink::DelayedChunk::Partition
        {
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token)
        });
    }

    finishDelayedChunk();
    delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);

    ++num_blocks_processed;
}

void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        bool added = false;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
        {
            auto lock = storage.lockParts();
            storage.fillNewPartName(part, lock);

            auto * deduplication_log = storage.getDeduplicationLog();
            if (deduplication_log)
            {
                const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);
                auto res = deduplication_log->addPart(block_id, part->info);
                if (!res.second)
                {
                    /// ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                    LOG_INFO(storage.log, "Block with ID {} already exists as part {}; ignoring it", block_id, res.first.getPartName());
                    continue;
                }
            }

            added = storage.renameTempPartAndAdd(part, transaction, lock);
            transaction.commit(&lock);
        }

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (added)
        {
            PartLog::addNewPart(storage.getContext(), part, partition.elapsed_ns);
            /// storage.incrementInsertedPartsProfileEvent(part->getType());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }
    }

    delayed_chunk.reset();
}

/// proton: starts
inline bool MergeTreeSink::ignorePartBlock(Int32 parts, Int32 part_index) const
{
    if (missing_seq_ranges.empty())
        return false;

    const auto & last_seq_range = missing_seq_ranges.back();
    if (last_seq_range.parts < 0)
        /// All parts are not committed last time
        return false;

    if (parts != last_seq_range.parts)
    {
        /// This shall not happen. If it does happen, the table partition algorithm
        /// in table definition has been changed. We just persist the blocks in favor
        /// of avoiding data loss (there can be data duplication)
        LOG_WARNING(
            storage.log,
            "Recovery phase. Expecting parts={}, but got={} for start_sn={}, end_sn={}. Partition algorithm is probably changed",
            parts,
            last_seq_range.parts,
            last_seq_range.start_sn,
            last_seq_range.end_sn);
        return false;
    }

    /// Recovery phase, only persist missing sequence ranges to avoid duplicate data
    for (const auto & seq_range : missing_seq_ranges)
    {
        if (seq_range.part_index == part_index)
        {
            /// The part block is in missing sequence ranges, persist it
            LOG_INFO(
                storage.log,
                "Recovery phase. Persisting missing parts={} part_index={} for start_sn={}, end_sn={}",
                parts,
                part_index,
                last_seq_range.start_sn,
                last_seq_range.end_sn);

            return false;
        }
    }

    LOG_INFO(
        storage.log,
        "Recovery phase. Skipping persisting parts={} part_index={} for start_sn={}, end_sn={} because it was previously persisted",
        parts,
        part_index,
        last_seq_range.start_sn,
        last_seq_range.end_sn);

    return true;
}
/// proton: ends

}
