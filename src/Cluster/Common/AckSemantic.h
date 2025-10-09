#pragma once

#include <Cluster/Common/SerdeTag.h>

#include <cstdint>

namespace cluster
{
SERDE enum AckSemantic : uint8_t {
    AfterProposal, /// ack immediately after proposing to Raft state machine (in-memory), ack caller
    /// AfterLeaderAppendWithQuorumSent, /// Ack after leader append the proposal to its log and sent the proposal to peers, we don't find this is meaningful
    QuorumCommitted, /// Ack after the quorum commits the proposal
};
}
