#pragma once

#include "LogSegment.h"

#include <NativeLog/Common/TopicShard.h>

#include <map>
#include <memory>
#include <shared_mutex>
#include <vector>

#include <boost/noncopyable.hpp>

namespace nlog
{
/// Index by base offset of a Segment and it is multi-thread safe
class LogSegments final : private boost::noncopyable
{
public:
    explicit LogSegments(const TopicShard & topic_shard_);

    void add(LogSegmentPtr segment);

    void remove(int64_t offset);

    LogSegmentPtr get(int64_t offset) const;

    bool contains(int64_t offset) const;

    /// @return The log segment associated with the smallest offset if it exists
    LogSegmentPtr firstSegment() const;

    /// @return The log segment associated with the greatest offset if it exists
    LogSegmentPtr lastSegment() const;

    LogSegmentPtr activeSegment() const { return lastSegment(); }

    /// smallest > offset
    /// @return The log segment with the smallest offset strictly greater
    /// than the given offset if it exists
    LogSegmentPtr higherSegment(int64_t offset) const;

    /// greatest < offset
    /// @return The log segment with the greatest offset strictly less than
    // the given offset if it exists
    LogSegmentPtr lowerSegment(int64_t offset) const;

    /// greatest offset <= offset
    /// @return The log segment with the greatest offset less than or equal to the
    /// given offset if it exists
    LogSegmentPtr floorSegment(int64_t offset) const;

    /// <= offset
    /// @return A list of segments which have base offset less or equal to the
    /// given offset if exists
    std::vector<LogSegmentPtr> lowerEqualSegments(int64_t offset) const;

    std::vector<int64_t> baseOffsets() const;

    /// @return a list of segments beginning with the segment that includes `from`
    ///         and ending with the segment that includes up to `to - 1` or the end of the log
    ///         if to > end of the log
    std::vector<LogSegmentPtr> values(int64_t from, int64_t to);

    std::vector<LogSegmentPtr> values();

    void apply(std::function<void(LogSegmentPtr &)> func);

    void close();

    void updateParentDir(const fs::path & parent_dir);

    void clear();

    bool empty() const;

    size_t size() const;

private:
    TopicShard topic_shard;

    mutable std::shared_mutex mlock;
    std::map<int64_t, LogSegmentPtr> segments;
};

using LogSegmentsPtr = std::shared_ptr<LogSegments>;
}
