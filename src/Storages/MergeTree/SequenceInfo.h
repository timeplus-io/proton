#pragma once

#include <common/types.h>
#include <vector>
#include <cassert>


namespace Poco
{
class Logger;
}

namespace DB
{
/// `SequenceInfo` represents `sn.txt` which are the sequence numbers associated
/// with a data part on file system. It also acts like a local checkpoint
/// for consumer of Distributed Write Ahead Log.
///
/// The `sn.txt` file services 2 main purposes
/// 1. Avoid duplicate data. the sequence of data ingestion for DistributedMergeTree is
///    a. Tailing the distributed write ahead log
///    b. Commit the data into parts in local file system
///    c. Commit the sequence numbers of consume data back to distributed write ahead log
/// There are race conditions between step b and step c : if the data commits to local file system
/// successfully, but failed to commit the corresponding sequence numbers back to distributed write ahead log
/// and system crashes, when system reboots, the same data will be consumed again and hence introducing
/// duplicate data. The `sn.txt` in local file system will guide the system which data already got
/// persistent to avoid this situation. Specifically when system reboots, each table will scan the checkpoint
/// files for all persistent sequence numbers, and produces a low watermark sequence number.
///
/// The low watermark sequence number will be fed back to distributed write ahead log and the
/// distributed write ahead log will start consuming data just from that sequence number.
/// Please note since step b above happens in parallel for performance, the sequence numbers committed
/// in local file system can be out of order and may have gap, when replaying the data from the
/// distributed write ahead log, and re-do step b and c, we need re-check if some part of the data is already
/// committed / covered, if that is the case, we will skip the committed data.
///
/// Here is one example. Please note that we are assuming `partition by` expression didn't get
/// changed during the this cause. If it does, nothing holds. One solution to prevent this situation from happening is
/// don't support changing partition by expression for DistributedMergeTree. The other way is stop all data ingestion
/// wait for all data committed, update the partition by expression and re-enable data ingestion which is troublesome.
///   a. Local file system committed sequence numbers : [100, 200], [300, 400], [401, 600] and system crashes or stops
///   b. System reboots and scan the sequence number checkpoint files and produces low watermark sequence number 201
///   c. In-memory, we also maintain a data structure for committed sequence number [300, 600]
///   d. The low watermark sequence number 201 is fed to the Distributed Write Ahead Log consumer and tailing the data
///   e. The code builds a block for sequence numbers in [201, 299]. Note the sequence range for the block
///      has to be exactly the same as missed in step 1, otherwise we may have missing data or duplicate data
///   f. Block with sequence numbers in [201, 299] will go through the same process (step b, c above) which will backfill
///      the missing data. There are sub-cases here as [201, 299] can be splitted into several partitions (although in
///      normal circumenstances, they will be in one partition) and hence resulting several blocks and some blocks may
///      be committed in local file system and some doesn't. The code needs honor these sub-cases as well.
///   g. The code also notices sequence [300, 600] is already committed, the tailing process just discard this data
///
/// 2. Consistent query. When shards are replicated, there will be lagging among shard replicas even they are tailing the
///    same Distributed Write Ahead Log. When a query is targeted for a shard, we need choose one shard replica to fulfill the request.
///    The sequence numbers checkpointing can guide us which shard to choose.
/// Here is one example.
///   a. Assume one shard has 3 replica : replica1, replica2 and replica3
///   b. replica1 commits sequence number 100; replica2 commits sequence number 101; replica3 commits sequence number 99
///   c. A query is targeted for this shard. The code will query the 3 replicas for the committed sequence number:
///      100, 101 and 99 will be returned respectively
///   d. The code then decides 99 is the lowest sequence number (as every replica committed it) and sends the query along with
///      sequence number 99 to any replica node.
///   e. Once a replica node receives the query and detects that a sequence number is sent along as well. It will scan the parts
///      whose associated sequence number less equal to 99. Please note this indeed requires the code to associate a correct sequence
///      number during part merging and the merging behavior is exactly the same among replicas. The merging machenism is documented
///      elsewhere

class ReadBuffer;
class WriteBuffer;

struct SequenceRange
{
    Int64 start_sn = -1;
    Int64 end_sn = -1;
    Int32 part_index = -1;
    Int32 parts = -1;

    bool valid() const
    {
        return start_sn >= 0 && end_sn >= 0 && start_sn <= end_sn
            && ((part_index >= 0 && parts >= 1 && part_index < parts) || (part_index == 0 && parts == 0));
    }

    SequenceRange() { }
    SequenceRange(Int64 start_seq_, Int64 end_seq_) : start_sn(start_seq_), end_sn(end_seq_) { }
    SequenceRange(Int32 part_index_, Int64 parts_) : part_index(part_index_), parts(parts_) { }
    SequenceRange(Int64 start_seq_, Int64 end_seq_, Int32 part_index_, Int64 parts_) : start_sn(start_seq_), end_sn(end_seq_), part_index(part_index_), parts(parts_) { }

    void write(WriteBuffer & out) const;
};

using IdempotentKey = std::pair<Int64, String>;
using IdempotentKeys = std::vector<IdempotentKey>;

bool operator==(const SequenceRange & lhs, const SequenceRange & rhs);
bool operator<(const SequenceRange & lhs, const SequenceRange & rhs);

using SequenceRanges = std::vector<SequenceRange>;

struct SequenceInfo
{
    SequenceRanges sequence_ranges;

    /// Associated idempotenent keys
    std::shared_ptr<IdempotentKeys> idempotent_keys;

    std::shared_ptr<SequenceInfo> shallowClone(Int32 part_index, Int32 parts) const
    {
        assert(sequence_ranges.size() == 1);

        SequenceRanges sparts{sequence_ranges};
        sparts[0].part_index = part_index;
        sparts[0].parts = parts;

        return std::make_shared<SequenceInfo>(std::move(sparts), idempotent_keys);
    }

    SequenceInfo(Int64 start_sn, Int64 end_sn, const std::shared_ptr<IdempotentKeys> & idempotent_keys_)
        : idempotent_keys(idempotent_keys_)
    {
        sequence_ranges.push_back(SequenceRange{start_sn, end_sn});
    }

    SequenceInfo(SequenceRanges sequence_ranges_, const std::shared_ptr<IdempotentKeys> & idempotent_keys_)
        : sequence_ranges(std::move(sequence_ranges_)), idempotent_keys(idempotent_keys_)
    {
    }

    bool valid() const;

    void write(WriteBuffer & out) const;

    static std::shared_ptr<SequenceInfo> read(ReadBuffer & in);
};

using SequenceInfoPtr = std::shared_ptr<SequenceInfo>;

/// Merge SequenceInfo into one according to last committed sn and max idempotent keys
SequenceInfoPtr
mergeSequenceInfo(std::vector<SequenceInfoPtr> & sequences, Int64 committed_sn, UInt64 max_idempotent_keys, Poco::Logger * log);

/// Find missing sequence ranges according to committed sn and return missing sequence ranges and new committed sn
std::pair<SequenceRanges, Int64> missingSequenceRanges(SequenceRanges & sequence_ranges, Int64 committed_sn, Poco::Logger * log);
}
