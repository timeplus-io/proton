#pragma once

#include <Cluster/Common/Entry.h>

namespace cluster::apply
{

/// `Task` continuously tails log entries, decode them and apply the log entries to application
/// state machine which was driven by a Raft replication group. It accepts log entries which
/// are committed to the replication group's (Raft) log and applies them to application to
/// advance to a new state.
///
/// After applying the log entries, one task is ack the waiting callers (clean proposals in proposal map).
///
/// All state transitions performed by the state machine are expected to be deterministic,
/// which ensures that if each instance is driven from the same consistent shared log,
/// they will all stay in sync.
///
/// Task could be implemented sync or async, thread-safe or non-thread-safe.
class Task
{
public:
    virtual ~Task() = default;

    virtual void startup() { }
    virtual void shutdown() { }

private:
    virtual void apply(cluster::EntryPtrs entries) = 0;
};

using TaskPtr = std::unique_ptr<Task>;
}
