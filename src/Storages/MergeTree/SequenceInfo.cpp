#include "SequenceInfo.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/parseIntStrict.h>
#include <common/logger_useful.h>

#include <boost/algorithm/string/join.hpp>
#include <Poco/Logger.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
inline SequenceRange parseSequenceRange(const String & s, String::size_type lpos, String::size_type rpos)
{
    SequenceRange seq_range;

    for (Int32 i = 0; i < 3; ++i)
    {
        auto comma_pos = s.find(',', lpos);
        if (comma_pos == String::npos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        if (comma_pos > rpos)
        {
            throw Exception("Invalid sequences " + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
        }

        switch (i)
        {
            case 0:
                seq_range.start_sn = parseIntStrict<Int64>(s, lpos, comma_pos);
                break;
            case 1:
                seq_range.end_sn  = parseIntStrict<Int64>(s, lpos, comma_pos);
                break;
            case 2:
                seq_range.part_index = parseIntStrict<Int32>(s, lpos, comma_pos);
                break;
        }

        lpos = comma_pos + 1;
    }

    seq_range.parts = parseIntStrict<Int32>(s, lpos, rpos);

    return seq_range;
}

SequenceRanges readSequenceRanges(ReadBuffer & in)
{
    assertString("seqs:", in);

    String data;
    DB::readText(data, in);

    if (data.empty())
    {
        return {};
    }

    SequenceRanges sequence_ranges;

    String::size_type siz = static_cast<String::size_type>(data.size());
    String::size_type lpos = 0;

    while (lpos < siz)
    {
        auto pos = data.find(';', lpos);
        if (pos == String::npos)
        {
            sequence_ranges.push_back(parseSequenceRange(data, lpos, siz));
            break;
        }
        else
        {
            sequence_ranges.push_back(parseSequenceRange(data, lpos, pos));
            lpos = pos + 1;
        }
    }

    return sequence_ranges;
}

inline IdempotentKey parseIdempotentKey(const String & s, String::size_type lpos, String::size_type rpos)
{
    IdempotentKey key;

    auto comma_pos = s.find(',', lpos);
    if (comma_pos == String::npos)
    {
        throw Exception("Invalid idempotent key" + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    if (comma_pos >= rpos)
    {
        throw Exception("Invalid idempotent key" + s, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }

    key.first = parseIntStrict<Int64>(s, lpos, comma_pos);
    key.second = String{s, comma_pos + 1, rpos - comma_pos - 1};

    return key;
}

std::shared_ptr<IdempotentKeys> readIdempotentKeys(ReadBuffer & in)
{
    assertString("keys:", in);

    String data;
    DB::readText(data, in);

    if (data.empty())
    {
        return {};
    }

    auto idempotent_keys = std::make_shared<IdempotentKeys>();

    String::size_type siz = static_cast<String::size_type>(data.size());
    String::size_type lpos = 0;

    for (; lpos < siz;)
    {
        auto pos = data.find(';', lpos);
        if (pos == String::npos)
        {
            idempotent_keys->push_back(parseIdempotentKey(data, lpos, siz));
            break;
        }
        else
        {
            idempotent_keys->push_back(parseIdempotentKey(data, lpos, pos));
            lpos = pos + 1;
        }
    }

    return idempotent_keys;
}

SequenceRanges mergeSequenceRanges(SequenceRanges & sequence_ranges, Int64 committed_sn, Poco::Logger * log)
{
    std::sort(sequence_ranges.begin(), sequence_ranges.end());

    SequenceRanges merged;
    SequenceRange last_seq_range;

    Int64 min_sn = -1;
    Int64 max_sn = -1;

    for (const auto & next_seq_range : sequence_ranges)
    {
        /// Merge ranges
        assert(next_seq_range.valid());

        if (min_sn == -1)
        {
            min_sn = next_seq_range.start_sn;
        }

        if (next_seq_range.end_sn > max_sn)
        {
            max_sn = next_seq_range.end_sn;
        }

        if (!last_seq_range.valid())
        {
            last_seq_range = next_seq_range;
        }
        else
        {
            /// There shall be no cases where there is sequence overlapping in a partition
            assert(last_seq_range.end_sn < next_seq_range.start_sn);
            if (last_seq_range.end_sn >= next_seq_range.start_sn)
            {
                LOG_ERROR(
                    log,
                    "Duplicate sn found: ({}, {}) -> ({}, {})",
                    last_seq_range.start_sn,
                    last_seq_range.end_sn,
                    next_seq_range.start_sn,
                    next_seq_range.end_sn);
            }
            last_seq_range = next_seq_range;
        }

        /// We don't check sequence gap here. There are 3 possible cases
        /// 1. The missing sequence is not committed yet (rarely)
        /// 2. It is casued by resending an / several idempotent blocks which will be deduped and ignored.
        /// 3. The Block whish has the missing sequence is distributed to a different partition
        /// Only for sequence ranges which are beyond the `committed_sn`, we need merge them and
        /// keep them around
        if (next_seq_range.end_sn > committed_sn)
        {
            merged.push_back(next_seq_range);
        }
    }

    if (log)
    {
        LOG_DEBUG(
            log,
            "Merge {} sequence ranges to {}, committed_sn={} min_sn={} max_sn={}",
            sequence_ranges.size(),
            merged.size(),
            committed_sn,
            min_sn,
            max_sn);
    }

    return merged;
}

inline std::shared_ptr<IdempotentKeys>
mergeIdempotentKeys(const std::set<IdempotentKey> & idempotent_keys, UInt64 max_idempotent_keys, Poco::Logger * log)
{
    if (idempotent_keys.empty())
    {
        return nullptr;
    }

    auto result = std::make_shared<IdempotentKeys>();

    auto keys_iter = idempotent_keys.begin();
    if (idempotent_keys.size() > max_idempotent_keys)
    {
        std::advance(keys_iter, idempotent_keys.size() - max_idempotent_keys);
        result->reserve(max_idempotent_keys);
    }
    else
    {
        result->reserve(idempotent_keys.size());
    }

    for (; keys_iter != idempotent_keys.end(); ++keys_iter)
    {
        result->push_back(*keys_iter);
    }

    if (log)
    {
        LOG_DEBUG(
            log,
            "Merge {} idempotent keys to {}, max_idempotent_keys={}",
            idempotent_keys.size(),
            result->size(),
            max_idempotent_keys);
    }

    return result;
}

inline void collectMissingSequenceRangeBefore(
    const SequenceRange & prev, size_t prev_index, Int64 next_expecting_sn, Poco::Logger * log, SequenceRanges & missing_ranges)
{
    if (prev_index != 0)
    {
        return;
    }

    if (prev.start_sn != next_expecting_sn)
    {
        /// There are sequence range missing before prev_range
        missing_ranges.emplace_back(next_expecting_sn, prev.start_sn - 1);
        assert(next_expecting_sn <= prev.start_sn - 1);

        if (log)
        {
            LOG_INFO(log, "Missing sn range=({}, {})", next_expecting_sn, prev.start_sn - 1);
        }
    }
}

inline void collectMissingSequenceRangeBetween(
    const SequenceRange & prev, const SequenceRange & cur, Poco::Logger * log, SequenceRanges & missing_ranges)
{
    if (prev.start_sn != cur.start_sn && prev.end_sn + 1 != cur.start_sn)
    {
        if (log)
        {
            LOG_INFO(log, "Missing sn range({}, {})", prev.end_sn + 1, cur.start_sn - 1);
        }

        assert(prev.end_sn + 1 <= cur.start_sn - 1);
        missing_ranges.emplace_back(prev.end_sn + 1, cur.start_sn - 1);
    }
}

inline void collectMissingPartsSequenceRange(
    const SequenceRanges & sequence_ranges, size_t prev_index, size_t cur_index, Poco::Logger * log, SequenceRanges & missing_ranges)
{
    Int32 next_expecting_part_index = 0;
    std::vector<String> missed_parts;

    for (size_t i = prev_index; i < cur_index; ++i)
    {
        if (sequence_ranges[i].part_index != next_expecting_part_index)
        {
            /// Collect the missing parts
            for (auto j = next_expecting_part_index; j != sequence_ranges[i].part_index; ++j)
            {
                SequenceRange range_copy = sequence_ranges[prev_index];
                range_copy.part_index = j;
                missing_ranges.push_back(range_copy);
                missed_parts.push_back(std::to_string(j));
            }
            next_expecting_part_index = sequence_ranges[i].part_index + 1;
        }
        else
        {
            ++next_expecting_part_index;
        }
    }

    for (;next_expecting_part_index < sequence_ranges[prev_index].parts; ++next_expecting_part_index)
    {
        SequenceRange range_copy = sequence_ranges[prev_index];
        range_copy.part_index = next_expecting_part_index;
        missing_ranges.push_back(range_copy);
        missed_parts.push_back(std::to_string(next_expecting_part_index));
    }

    if (log)
    {
        LOG_INFO(
            log,
            "Not all parts in sn range=({}, {}) are committed, total parts={}, missed parts=({})",
            sequence_ranges[prev_index].start_sn,
            sequence_ranges[prev_index].end_sn,
            sequence_ranges[prev_index].parts,
            boost::algorithm::join(missed_parts, ", "));
    }
}

inline void collectMissingSequenceRanges(
    const SequenceRanges & sequence_ranges,
    size_t prev_index,
    size_t cur_index,
    Int32 parts,
    Poco::Logger * log,
    Int64 & next_expecting_sn,
    SequenceRanges & missing_ranges)
{
    const auto & prev_range = sequence_ranges[prev_index];
    auto index = cur_index;
    if (cur_index == sequence_ranges.size())
    {
        index = cur_index - 1;
    }
    const auto & cur_range = sequence_ranges[index];

    /// Meet a new start_sn, calcuate if the parts in prev_range
    /// are all committed. There are several cases
    /// 1. There are sequence gaps before prev_range
    /// 2. There are part missing in current prev_range
    /// 3. There are sequence gaps between prev_range and current range
    if (prev_range.parts == parts)
    {
        /// All parts in this prev_range are committed
        assert (prev_range.start_sn >= next_expecting_sn);

        if (prev_range.start_sn == next_expecting_sn)
        {
            /// If we can progress sn, it means ther are no sequence missing
            /// before prev_range. Progress next expecting sn
            next_expecting_sn = prev_range.end_sn + 1;

            if (log)
            {
                LOG_INFO(
                    log,
                    "Parts in sn range=({}, {}) having total parts={} are all committed. Progressing next expecting sn to "
                    "{}",
                    prev_range.start_sn,
                    prev_range.end_sn,
                    prev_range.parts,
                    prev_range.end_sn + 1);
            }
        }
        else
        {
            /// Check if there are sequence range missing before prev_range
            collectMissingSequenceRangeBefore(prev_range, prev_index, next_expecting_sn, log, missing_ranges);
        }

        /// Check if there are sequence range missing between pre_range and cur_range
        collectMissingSequenceRangeBetween(prev_range, cur_range, log, missing_ranges);
    }
    else
    {
        /// 1) Hanlde missing sequence range before prev_range
        collectMissingSequenceRangeBefore(prev_range, prev_index, next_expecting_sn, log, missing_ranges);

        /// 2) Handle missing parts in prev_range
        /// Some parts in prev_range are missing
        /// Find the missing parts and add to `missing_ranges`
        collectMissingPartsSequenceRange(sequence_ranges, prev_index, cur_index, log, missing_ranges);

        /// 3) Handle missing sequence range between prev_range and cur_range
        collectMissingSequenceRangeBetween(prev_range, cur_range, log, missing_ranges);
    }
}
}

bool operator==(const SequenceRange & lhs, const SequenceRange & rhs)
{
    return lhs.start_sn == rhs.start_sn && lhs.end_sn == rhs.end_sn && lhs.part_index == rhs.part_index && lhs.parts == rhs.parts;
}

bool operator<(const SequenceRange & lhs, const SequenceRange & rhs)
{
    if (lhs.start_sn < rhs.start_sn)
    {
        return true;
    }
    else if (lhs.start_sn == rhs.start_sn)
    {
        return lhs.part_index < rhs.part_index;
    }
    else
    {
        return false;
    }
}

inline void SequenceRange::write(WriteBuffer & out) const
{
    DB::writeText(start_sn, out);
    DB::writeText(",", out);
    DB::writeText(end_sn, out);
    DB::writeText(",", out);
    DB::writeText(part_index, out);
    DB::writeText(",", out);
    DB::writeText(parts, out);
}

bool SequenceInfo::valid() const
{
    if (sequence_ranges.empty() && (!idempotent_keys || idempotent_keys->empty()))
    {
        return false;
    }

    for (const auto & seq_range: sequence_ranges)
    {
        if (!seq_range.valid())
        {
            return false;
        }
    }

    return true;
}

void SequenceInfo::write(WriteBuffer & out) const
{
    if (!valid())
    {
        return;
    }

    /// Format:
    /// version
    /// seqs:start_sn,end_sn,part_index,parts;
    /// keys:a,sn;b,sn;c,sn

    /// Version
    DB::writeText("1\n", out);

    DB::writeText("seqs:", out);
    /// Sequence ranges
    for (size_t index = 0, siz = sequence_ranges.size(); index < siz;)
    {
        sequence_ranges[index].write(out);
        if (++index < siz)
        {
            DB::writeText(";", out);
        }
    }

    if (!idempotent_keys)
    {
        out.finalize();
        return;
    }

    DB::writeText("\n", out);

    DB::writeText("keys:", out);
    /// Idempotent keys
    for (size_t index = 0, siz = idempotent_keys->size(); index < siz;)
    {
        DB::writeText(idempotent_keys->at(index).first, out);
        DB::writeText(",", out);
        DB::writeText(idempotent_keys->at(index).second, out);
        if (++index < siz)
        {
            DB::writeText(";", out);
        }
    }
    out.finalize();
}

std::shared_ptr<SequenceInfo> SequenceInfo::read(ReadBuffer & in)
{
    assertString("1\n", in);

    auto sequence_ranges = readSequenceRanges(in);

    std::shared_ptr<IdempotentKeys> idempotent_keys;
    if (!in.eof())
    {
        assertString("\n", in);
        idempotent_keys = readIdempotentKeys(in);
    }

    return std::make_shared<SequenceInfo>(std::move(sequence_ranges), idempotent_keys);
}

/// Data in parameter `sequences` will be reordered when merging
SequenceInfoPtr
mergeSequenceInfo(std::vector<SequenceInfoPtr> & sequences, Int64 committed_sn, UInt64 max_idempotent_keys, Poco::Logger * log)
{
    if (sequences.empty())
    {
        return nullptr;
    }

    SequenceRanges sequence_ranges;
    std::set<IdempotentKey> idempotent_keys;

    for (const auto & seq_info : sequences)
    {
        for (const auto & seq_range : seq_info->sequence_ranges)
        {
            /// We don't filter out sequence range according to `committed_sn` here
            /// because we like to do validation
            sequence_ranges.push_back(seq_range);
        }

        if (seq_info->idempotent_keys)
        {
            for (const auto & key : *seq_info->idempotent_keys)
            {
                idempotent_keys.insert(key);
            }
        }
    }

    auto merged_ranges = mergeSequenceRanges(sequence_ranges, committed_sn, log);
    auto merged_keys = mergeIdempotentKeys(idempotent_keys, max_idempotent_keys, log);

    return std::make_shared<SequenceInfo>(std::move(merged_ranges), merged_keys);
}

/// The `sequence_ranges` are assumed to have sequence numbers which are great than `committed`
std::pair<SequenceRanges, Int64>
missingSequenceRanges(SequenceRanges & sequence_ranges, Int64 committed, Poco::Logger * log)
{
    if (sequence_ranges.empty())
    {
        return {{}, committed + 1};
    }

    std::sort(sequence_ranges.begin(), sequence_ranges.end());

    SequenceRanges missing_ranges;
    auto next_expecting_sn = committed + 1;
    size_t prev_index = 0;
    Int32 parts = 1;

    for (size_t cur_index = 1, siz = sequence_ranges.size(); cur_index < siz; ++cur_index)
    {
        const auto & prev_range = sequence_ranges[prev_index];
        const auto & cur_range = sequence_ranges[cur_index];

        if (prev_range.start_sn == cur_range.start_sn)
        {
            assert(prev_range.end_sn == cur_range.end_sn);
            assert(prev_range.part_index < cur_range.part_index);
            ++parts;

            continue;
        }

        collectMissingSequenceRanges(sequence_ranges, prev_index, cur_index, parts, log, next_expecting_sn, missing_ranges);

        /// Since here it starts a new start_sn, update prev_index and parts
        prev_index = cur_index;
        parts = 1;
    }

    /// The last range
    collectMissingSequenceRanges(sequence_ranges, prev_index, sequence_ranges.size(), parts, log, next_expecting_sn, missing_ranges);

    return {std::move(missing_ranges), next_expecting_sn};
}
}
