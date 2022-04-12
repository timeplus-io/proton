#pragma once

#include "LogSegment.h"

#include <NativeLog/Common/StreamShard.h>

#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>

#include <boost/noncopyable.hpp>

namespace nlog
{
/// Index by base sn of a Segment and it is multi-thread safe
class LogSegments final : private boost::noncopyable
{
public:
    explicit LogSegments(const StreamShard & stream_shard_);

    void add(LogSegmentPtr segment);

    void remove(int64_t sn);

    LogSegmentPtr get(int64_t sn) const;

    bool contains(int64_t sn) const;

    /// @return The log segment associated with the smallest sn if it exists
    LogSegmentPtr firstSegment() const;

    /// @return The log segment associated with the greatest sn if it exists
    LogSegmentPtr lastSegment() const;

    LogSegmentPtr activeSegment() const { return lastSegment(); }

    /// smallest > sn
    /// @return The log segment with the smallest sn strictly greater
    /// than the given sn if it exists
    LogSegmentPtr higherSegment(int64_t sn) const;

    /// greatest < sn
    /// @return The log segment with the greatest sn strictly less than
    // the given sn if it exists
    LogSegmentPtr lowerSegment(int64_t sn) const;

    /// greatest sn <= sn
    /// @return The log segment with the greatest sn less than or equal to the
    /// given sn if it exists
    LogSegmentPtr floorSegment(int64_t sn) const;

    /// <= sn
    /// @return A list of segments which have base sn less or equal to the
    /// given sn if exists
    std::vector<LogSegmentPtr> lowerEqualSegments(int64_t sn) const;

    std::vector<int64_t> baseSequences() const;

    /// @return a list of segments beginning with the segment that includes `from`
    ///         and ending with the segment that includes up to `to - 1` or the end of the log
    ///         if to > end of the log
    std::vector<LogSegmentPtr> values(int64_t from, int64_t to);

    std::vector<LogSegmentPtr> values();

    void apply(std::function<bool(LogSegmentPtr &)> func);

    void close();

    void updateParentDir(const fs::path & parent_dir);

    void clear();

    bool empty() const;

    size_t size() const;

private:
    StreamShard stream_shard;

    mutable std::shared_mutex mlock;
    std::map<int64_t, LogSegmentPtr> segments;
};

using LogSegmentsPtr = std::shared_ptr<LogSegments>;
}
