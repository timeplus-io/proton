#include <Cluster/MetaStore/ProposalRegistry.h>

#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

namespace cluster::meta
{

ProposalRegistry::ProposalRegistry(LoggerPtr logger_) : logger(logger_)
{
}

ProposalRegistry::~ProposalRegistry()
{
    std::lock_guard lock(mutex);
    /// Cancel all pending proposals on destruction
    for (auto & [correlation_id, proposal] : pending_proposals)
    {
        try
        {
            proposal->promise.set_value(Error(DB::ErrorCodes::LOGICAL_ERROR, "ProposalRegistry destroyed"));
        }
        catch (...)
        {
            /// Promise may already be satisfied
        }
    }
}

std::future<Error> ProposalRegistry::registerProposal(uint64_t correlation_id)
{
    std::lock_guard lock(mutex);
    
    auto [it, inserted] = pending_proposals.emplace(
        correlation_id, 
        std::make_unique<PendingProposal>(PendingProposal{
            .promise = std::promise<Error>(),
            .registered_at = std::chrono::steady_clock::now()
        })
    );
    
    if (!inserted)
    {
        LOG_ERROR(logger, "Duplicate correlation_id={} in registerProposal", correlation_id);
        std::promise<Error> error_promise;
        error_promise.set_value(Error(DB::ErrorCodes::LOGICAL_ERROR, "Duplicate correlation ID"));
        return error_promise.get_future();
    }
    
    LOG_DEBUG(logger, "Registered proposal with correlation_id={}", correlation_id);
    return it->second->promise.get_future();
}

void ProposalRegistry::cancelProposal(uint64_t correlation_id, const Error & error)
{
    std::unique_ptr<PendingProposal> proposal;
    
    {
        std::lock_guard lock(mutex);
        auto it = pending_proposals.find(correlation_id);
        if (it == pending_proposals.end())
        {
            LOG_DEBUG(logger, "Cannot cancel non-existent proposal correlation_id={}", correlation_id);
            return;
        }
        proposal = std::move(it->second);
        pending_proposals.erase(it);
    }

    /// Set the promise outside the lock to avoid potential deadlock
    try
    {
        proposal->promise.set_value(error);
        LOG_DEBUG(logger, "Cancelled proposal correlation_id={} with error={}", correlation_id, error.string());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(logger, "Failed to set promise for correlation_id={}: {}", correlation_id, e.what());
    }
}

void ProposalRegistry::completeProposal(uint64_t correlation_id)
{
    std::unique_ptr<PendingProposal> proposal;
    
    {
        std::lock_guard lock(mutex);
        auto it = pending_proposals.find(correlation_id);
        if (it == pending_proposals.end())
        {
            LOG_DEBUG(logger, "Cannot complete non-existent proposal correlation_id={}", correlation_id);
            return;
        }
        proposal = std::move(it->second);
        pending_proposals.erase(it);
    }

    /// Set the promise outside the lock
    try
    {
        proposal->promise.set_value(Error()); // Success - no error
        LOG_DEBUG(logger, "Completed proposal correlation_id={}", correlation_id);
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(logger, "Failed to set promise for correlation_id={}: {}", correlation_id, e.what());
    }
}

void ProposalRegistry::cleanupExpired()
{
    std::vector<std::pair<uint64_t, std::unique_ptr<PendingProposal>>> expired;
    auto now = std::chrono::steady_clock::now();
    
    {
        std::lock_guard lock(mutex);
        for (auto it = pending_proposals.begin(); it != pending_proposals.end();)
        {
            if (now - it->second->registered_at > PROPOSAL_TIMEOUT)
            {
                expired.emplace_back(it->first, std::move(it->second));
                it = pending_proposals.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }

    /// Set promises outside the lock
    for (auto & [correlation_id, proposal] : expired)
    {
        try
        {
            proposal->promise.set_value(Error(DB::ErrorCodes::TIMEOUT_EXCEEDED, "Proposal timeout"));
            LOG_WARNING(logger, "Proposal correlation_id={} timed out", correlation_id);
        }
        catch (...)
        {
            /// Promise may already be satisfied
        }
    }
    
    if (!expired.empty())
    {
        LOG_INFO(logger, "Cleaned up {} expired proposals", expired.size());
    }
}

size_t ProposalRegistry::pendingCount() const
{
    std::lock_guard lock(mutex);
    return pending_proposals.size();
}

}
