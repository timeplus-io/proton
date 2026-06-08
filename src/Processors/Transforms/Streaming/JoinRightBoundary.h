#pragma once

#include <Cluster/Common/Constants.h>
#include <Processors/Streaming/ISource.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <fmt/ranges.h>

namespace DB::Streaming
{

class JoinRightBoundary;
using JoinRightBoundaryPtr = std::shared_ptr<JoinRightBoundary>;

class JoinLeftBoundary;
using JoinLeftBoundaryPtr = std::shared_ptr<JoinLeftBoundary>;

class JoinRightBoundary
{
public:
    JoinRightBoundary(std::vector<std::shared_ptr<ISource>> sources_, std::vector<Int64> stop_sns_, size_t num_participants_)
        : sources(std::move(sources_))
        , stop_sns(std::move(stop_sns_))
        , participants_reached(num_participants_, false)
        , num_participants(num_participants_)
    {
        chassert(sources.size() == stop_sns.size());
        chassert(num_participants > 0);

        for (size_t i = 0; i < sources.size(); ++i)
        {
            /// Empty right-snapshot shards use stop SN 0. They still need a non-terminal stop so live rows cannot
            /// pass the split boundary until every left participant has reached the matching historical boundary.
            sources[i]->setStopSN(stop_sns[i], /*terminal=*/false);
        }
    }

    ~JoinRightBoundary()
    {
        if (!isReleased())
        {
            for (const auto & source : sources)
                source->clearStopSN();
        }
    }

    bool markParticipantReached(size_t participant)
    {
        {
            std::lock_guard lock(mutex);
            if (participant >= participants_reached.size())
                return isReleased();

            if (!participants_reached[participant])
            {
                participants_reached[participant] = true;
                ++num_reached_participants;
            }
        }

        return tryRelease();
    }

    bool release()
    {
        if (released.exchange(true, std::memory_order_acq_rel))
            return false;

        for (const auto & source : sources)
            source->clearStopSN();

        return true;
    }

    bool tryRelease()
    {
        if (isReleased())
            return false;

        {
            std::lock_guard lock(mutex);
            if (num_reached_participants < num_participants)
                return false;
        }

        for (size_t i = 0; i < sources.size(); ++i)
        {
            if (stop_sns[i] >= cluster::Constants::LogStartSN && sources[i]->lastProcessedSN() < stop_sns[i])
                return false;
        }

        return release();
    }

    bool isReleased() const { return released.load(std::memory_order_acquire); }

    String progressString() const
    {
        std::vector<String> progress;
        progress.reserve(sources.size());
        for (size_t i = 0; i < sources.size(); ++i)
            progress.push_back(fmt::format("{}:{}", sources[i]->lastProcessedSN(), stop_sns[i]));
        size_t reached;
        {
            std::lock_guard lock(mutex);
            reached = num_reached_participants;
        }
        return fmt::format("sources=[{}], participants={}/{}", fmt::join(progress, ", "), reached, num_participants);
    }

private:
    mutable std::mutex mutex;
    std::vector<std::shared_ptr<ISource>> sources;
    std::vector<Int64> stop_sns;
    std::vector<bool> participants_reached;
    size_t num_participants;
    size_t num_reached_participants = 0;
    std::atomic_bool released = false;
};

class JoinLeftBoundary
{
public:
    explicit JoinLeftBoundary(size_t num_participants_) : ended(num_participants_, false), num_participants(num_participants_)
    {
        chassert(num_participants > 0);
    }

    bool markParticipantEnded(size_t participant)
    {
        std::lock_guard lock(mutex);

        if (participant >= ended.size())
            return false;

        if (ended[participant])
            return released.load(std::memory_order_acquire);

        ended[participant] = true;
        ++num_ended;
        if (num_ended == num_participants)
            released.store(true, std::memory_order_release);

        return released.load(std::memory_order_acquire);
    }

    bool release()
    {
        std::lock_guard lock(mutex);

        if (released.exchange(true, std::memory_order_acq_rel))
            return false;

        std::fill(ended.begin(), ended.end(), true);
        num_ended = num_participants;
        return true;
    }

    bool isReleased() const { return released.load(std::memory_order_acquire); }

    String progressString() const
    {
        std::lock_guard lock(mutex);
        return fmt::format("{}/{}", num_ended, num_participants);
    }

private:
    mutable std::mutex mutex;
    std::vector<bool> ended;
    size_t num_participants;
    size_t num_ended = 0;
    std::atomic_bool released = false;
};

}
