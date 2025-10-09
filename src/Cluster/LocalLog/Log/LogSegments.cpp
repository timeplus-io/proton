#include <Cluster/LocalLog/Log/LogSegments.h>

namespace cluster::nlog
{
/// Index by base sn of a Segment and it is multi-thread safe
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

/// \return the segment with biggest base sn, whose max sn in the segment is less than the given sn
LogSegmentPtr LogSegments::lowerSegment(int64_t sn) const
{
    std::shared_lock guard{mlock};
    auto iter = segments.lower_bound(sn);
    if (iter == segments.end())
        return {};

    if (iter != segments.begin())
        /// lower_bound returns first key >= sn
        /// back off one item to get the last maximum key
        /// which is less than sn
        return (--iter)->second;

    return {};
}

/// \return the first segment which possible contains sn
LogSegmentPtr LogSegments::floorSegment(int64_t sn) const
{
    std::shared_lock guard{mlock};
    if (segments.empty())
        return {};

    auto iter = segments.lower_bound(sn);
    if (iter == segments.end())
        /// There is no segment base sn which is >= sn
        /// return the last segment whose sns are possibly
        /// all less or less equal to the target sn or
        /// contains the target sn
        return segments.rbegin()->second;

    /// lower_bound returns first base sn >= sn
    if (iter->first == sn)
        return iter->second;

    if (iter != segments.begin())
        return (--iter)->second;

    return {};
}

/// <= sn
std::vector<LogSegmentPtr> LogSegments::lowerEqualSegments(int64_t sn) const
{
    std::vector<LogSegmentPtr> results;

    std::shared_lock guard{mlock};

    auto iter_end = segments.lower_bound(sn);
    if (iter_end != segments.end())
    {
        if (iter_end->first == sn)
        {
            /// Found key == sn, progress iter to next item which is strictly > sn
            ++iter_end;
        }
        else
        {
            /// Found key > sn, if it is the first item, then there are no such
            /// key <= sn
            if (iter_end == segments.begin())
                return results;
        }
    }

    for (auto iter_begin = segments.begin(); iter_begin != iter_end; ++iter_begin)
        results.push_back(iter_begin->second);

    assert(results.back()->baseSequence() <= sn);
    return results;
}

/// > sn
std::vector<LogSegmentPtr> LogSegments::upperSegments(int64_t sn) const
{
    std::vector<LogSegmentPtr> results;

    std::shared_lock guard{mlock};

    auto iter_begin = segments.upper_bound(sn);

    for (auto iter_end = segments.end(); iter_begin != iter_end; ++iter_begin)
        results.push_back(iter_begin->second);

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

/// \return a list of segments beginning with the segment that includes `from`
///         and ending with the segment that includes up to `to - 1` or the end of the log
///         if to > end of the log, [from, to)
std::vector<LogSegmentPtr> LogSegments::segmentsBetween(int64_t from, int64_t to) const
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

std::vector<LogSegmentPtr> LogSegments::allSegments() const
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

void LogSegments::updateParentDir(const fs::path & parent_dir)
{
    apply([&parent_dir](LogSegmentPtr & segment) {
        segment->updateParentDir(parent_dir);
        return false;
    });
}

void LogSegments::clear() noexcept
{
    std::unique_lock guard{mlock};
    segments.clear();
}

bool LogSegments::empty() const noexcept
{
    std::shared_lock guard{mlock};
    return segments.empty();
}

size_t LogSegments::size() const noexcept
{
    std::shared_lock guard{mlock};
    return segments.size();
}

}
