#include "LogSegments.h"

namespace nlog
{
/// Index by base offset of a Segment and it is multi-thread safe
LogSegments::LogSegments(const TopicShard & topic_shard_) : topic_shard(topic_shard_) { }

void LogSegments::add(LogSegmentPtr segment)
{
    std::unique_lock guard{mlock};
    segments[segment->baseOffset()] = std::move(segment);
}

void LogSegments::remove(int64_t offset)
{
    std::unique_lock guard{mlock};
    segments.erase(offset);
}

LogSegmentPtr LogSegments::get(int64_t offset) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.find(offset);
    if (iter != segments.end())
        return iter->second;

    return {};
}

bool LogSegments::contains(int64_t offset) const
{
    std::shared_lock guard{mlock};
    return segments.contains(offset);
}

LogSegmentPtr LogSegments::firstSegment() const
{
    std::shared_lock guard{mlock};
    if (segments.empty())
        return {};

    return segments.begin()->second;
}

LogSegmentPtr LogSegments::lastSegment() const
{
    std::shared_lock guard{mlock};
    if (segments.empty())
        return {};

    return segments.rbegin()->second;
}

/// strictly > offset
LogSegmentPtr LogSegments::higherSegment(int64_t offset) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.upper_bound(offset);
    if (iter != segments.end())
        return iter->second;

    return {};
}

/// < offset
LogSegmentPtr LogSegments::lowerSegment(int64_t offset) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.lower_bound(offset);
    if (iter == segments.end())
        return {};

    if (iter != segments.begin())
        /// lower_bound returns first key >= offset
        /// back off one item to get the last maximum key
        /// which is greater than < offset
        return (--iter)->second;

    return {};
}

/// greatest offset <= offset
LogSegmentPtr LogSegments::floorSegment(int64_t offset) const
{
    std::shared_lock guard{mlock};
    if (segments.empty())
        return {};

    auto iter = segments.lower_bound(offset);
    if (iter == segments.end())
        /// There are no keys >= offset
        /// return the last segment
        return segments.rbegin()->second;

    /// lower_bound returns first key >= offset
    if (iter->first == offset)
        return iter->second;

    if (iter != segments.begin())
        return (--iter)->second;

    return {};
}

/// <= offset
std::vector<LogSegmentPtr> LogSegments::lowerEqualSegments(int64_t offset) const
{
    std::shared_lock guard{mlock};

    auto iter = segments.lower_bound(offset);
    if (iter != segments.end())
    {
        if (iter->first == offset)
        {
            /// Found key == offset, progress iter to next item which is strictly > offset
            ++iter;
        }
        else
        {
            /// Found key > offset, if it is the first item, then there are no such
            /// key <= offset
            if (iter == segments.begin())
                return {};
        }

    }

    std::vector<LogSegmentPtr> results;
    for (auto iter_begin = segments.begin(); iter_begin != iter; ++iter_begin)
        results.push_back(iter->second);

    return results;
}

std::vector<int64_t> LogSegments::baseOffsets() const
{
    std::vector<int64_t> base_offsets;

    std::shared_lock guard{mlock};
    base_offsets.reserve(segments.size());
    for (const auto & item : segments)
        base_offsets.push_back(item.second->baseOffset());

    return base_offsets;
}

/// @return a list of segments beginning with the segment that includes `from`
///         and ending with the segment that includes up to `to - 1` or the end of the log
///         if to > end of the log
std::vector<LogSegmentPtr> LogSegments::values(int64_t from, int64_t to)
{
    if (to <= from)
        return {};

    std::shared_lock guard{mlock};

    /// >= from
    auto iter = segments.lower_bound(from);
    if (iter == segments.end() || iter->first >= to)
        return {};

    /// >= to
    std::vector<LogSegmentPtr> results;
    auto high_iter = segments.lower_bound(to);
    for (; iter != high_iter; ++iter)
        results.push_back(iter->second);

    return results;
}

std::vector<LogSegmentPtr> LogSegments::values()
{
    std::vector<LogSegmentPtr> results;

    std::shared_lock guard{mlock};
    results.reserve(segments.size());
    for (const auto & item : segments)
        results.push_back(item.second);

    return results;
}

void LogSegments::apply(std::function<void(LogSegmentPtr &)> func)
{
    std::shared_lock guard{mlock};
    for (auto & item : segments)
        func(item.second);
}

void LogSegments::close()
{
    std::shared_lock guard{mlock};
    for (auto & item : segments)
        item.second->close();
}

void LogSegments::updateParentDir(const fs::path & parent_dir)
{
    apply([&parent_dir](LogSegmentPtr & segment) { segment->updateParentDir(parent_dir); });
}

void LogSegments::clear()
{
    std::unique_lock guard{mlock};
    segments.clear();
}

bool LogSegments::empty() const
{
    std::shared_lock guard{mlock};
    return segments.empty();
}

size_t LogSegments::size() const
{
    std::shared_lock guard{mlock};
    return segments.size();
}

}
