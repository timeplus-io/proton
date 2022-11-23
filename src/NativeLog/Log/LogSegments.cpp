#include "LogSegments.h"

namespace nlog
{
/// Index by base sn of a Segment and it is multi-thread safe
LogSegments::LogSegments(const StreamShard & stream_shard_) : stream_shard(stream_shard_) { }

void LogSegments::add(LogSegmentPtr segment)
{
    std::unique_lock guard{mlock};
    segments[segment->baseSequence()] = std::move(segment);
}

void LogSegments::remove(int64_t sn)
{
    std::unique_lock guard{mlock};
    segments.erase(sn);
}

LogSegmentPtr LogSegments::get(int64_t sn) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.find(sn);
    if (iter != segments.end())
        return iter->second;

    return {};
}

bool LogSegments::contains(int64_t sn) const
{
    std::shared_lock guard{mlock};
    return segments.contains(sn);
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

/// strictly > sn
LogSegmentPtr LogSegments::higherSegment(int64_t sn) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.upper_bound(sn);
    if (iter != segments.end())
        return iter->second;

    return {};
}

/// < sn
LogSegmentPtr LogSegments::lowerSegment(int64_t sn) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.lower_bound(sn);
    if (iter == segments.end())
        return {};

    if (iter != segments.begin())
        /// lower_bound returns first key >= sn
        /// back off one item to get the last maximum key
        /// which is greater than < sn
        return (--iter)->second;

    return {};
}

/// greatest sn <= sn
LogSegmentPtr LogSegments::floorSegment(int64_t sn) const
{
    std::shared_lock guard{mlock};
    if (segments.empty())
        return {};

    auto iter = segments.lower_bound(sn);
    if (iter == segments.end())
        /// There are no keys >= sn
        /// return the last segment
        return segments.rbegin()->second;

    /// lower_bound returns first key >= sn
    if (iter->first == sn)
        return iter->second;

    if (iter != segments.begin())
        return (--iter)->second;

    return {};
}

/// <= sn
std::vector<LogSegmentPtr> LogSegments::lowerEqualSegments(int64_t sn) const
{
    std::shared_lock guard{mlock};

    auto iter = segments.lower_bound(sn);
    if (iter != segments.end())
    {
        if (iter->first == sn)
        {
            /// Found key == sn, progress iter to next item which is strictly > sn
            ++iter;
        }
        else
        {
            /// Found key > sn, if it is the first item, then there are no such
            /// key <= sn
            if (iter == segments.begin())
                return {};
        }

    }

    std::vector<LogSegmentPtr> results;
    for (auto iter_begin = segments.begin(); iter_begin != iter; ++iter_begin)
        results.push_back(iter->second);

    return results;
}

std::vector<int64_t> LogSegments::baseSequences() const
{
    std::vector<int64_t> base_sns;

    std::shared_lock guard{mlock};
    base_sns.reserve(segments.size());
    for (const auto & item : segments)
        base_sns.push_back(item.second->baseSequence());

    return base_sns;
}

std::pair<int64_t, int64_t> LogSegments::sequenceRange() const
{
    std::pair<int64_t, int64_t> sn_range{-1, -1};

    std::shared_lock guard{mlock};
    if (segments.empty())
        return sn_range;

    sn_range.first = segments.begin()->second->baseSequence();
    sn_range.second = segments.rbegin()->second->baseSequence();

    return sn_range;
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

void LogSegments::apply(std::function<bool(LogSegmentPtr &)> func)
{
    std::shared_lock guard{mlock};
    for (auto & item : segments)
        if (func(item.second))
            break;
}

void LogSegments::close()
{
    std::shared_lock guard{mlock};
    for (auto & item : segments)
        item.second->close();
}

void LogSegments::updateParentDir(const fs::path & parent_dir)
{
    apply([&parent_dir](LogSegmentPtr & segment) { segment->updateParentDir(parent_dir); return false; });
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
