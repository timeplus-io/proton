#pragma once

#include <Cluster/Common/Error.h>
#include <Common/Logger.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace cluster::meta
{
/// Registry for tracking metadata proposals and correlating acks
/// Provides synchronous proposal completion
class ProposalRegistry
{
public:
    explicit ProposalRegistry(LoggerPtr logger_);
    ~ProposalRegistry();

    /// Register a new proposal with correlation ID
    /// Returns a future that will be completed when the proposal is acked
    std::future<Error> registerProposal(uint64_t correlation_id);

    /// Cancel a proposal (e.g., on timeout or error)
    void cancelProposal(uint64_t correlation_id, const Error & error);

    /// Complete a proposal successfully
    void completeProposal(uint64_t correlation_id);

    /// Cleanup expired proposals
    void cleanupExpired();

    /// Get number of pending proposals (for metrics/monitoring)
    size_t pendingCount() const;

private:
    struct PendingProposal
    {
        std::promise<Error> promise;
        std::chrono::steady_clock::time_point registered_at;
    };

    mutable std::mutex mutex;
    std::unordered_map<uint64_t, std::unique_ptr<PendingProposal>> pending_proposals;
    LoggerPtr logger;

    static constexpr std::chrono::seconds PROPOSAL_TIMEOUT{30};
};

using ProposalRegistryPtr = std::shared_ptr<ProposalRegistry>;

}
