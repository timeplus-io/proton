#pragma once

#include "Cluster.h"
#include "Record.h"
#include "Results.h"

#include <any>

namespace DWAL
{
/** Distributed Write Ahead Log (WAL) interfaces which defines an ordered sequence of `transitions`.
 * At its core, it is an sequntial orderded and append-only log abstraction
 * It generally can store any `transition` operation including but not limited by the following ones,
 * as long as the operation can wrap in a `Block` and can be understood in the all partiticipants invovled.
 * 1. Insert a data block (data path)
 * 2. Mutate commands like `ALTER TABLE table UPDATE ...`
 * 3. DDL commands like `CREATE TABLE table ...`
 * 4. ...
 */

class WAL : private boost::noncopyable
{
public:
    virtual ~WAL() = default;
    virtual void startup();
    virtual void shutdown();
    virtual String type() const = 0;

    /// Append a Record to the target WAL and returns SequenceNumber for this record
    /// Once this function is returned without an error, the record is guaranteed to be committed
    virtual AppendResult append(const Record & record, std::any & ctx) = 0;

    /// Append a record to the target WAL asynchously and register a callback which will be invoked
    /// when the append result is ready for this record. Return `0` if the append request is recorded
    /// otherwise return non-zero and `callback` may not be invoked in this case.
    /// However please also note in normal case, the `callback` is probably invoked
    /// in a separated thread in a future time, so make sure callback is multi-thread safe and no throws
    virtual Int32 append(const Record & record, AppendCallback callback, void * data, std::any & ctx) = 0;

    /// Poll status for appended record for async `append`
    virtual void poll(Int32 timeout_ms, std::any & ctx) = 0;

    /// Consume records in WAL. For each record, invoke `callback`
    /// return false if failed, otherwise return true.
    /// `record` is moved to `ConsumeCallback` which means `ConsumeCallback` will have
    /// the ownership of `record`. ConsumeCallback is expected to multi-thread safe
    /// and fast
    virtual Int32 consume(ConsumeCallback callback, void * data, std::any & ctx) = 0;

    /// Consume records in WAL either `timeout_ms` reached or maximum `count` records
    /// have been consumed. The ownership of the `records` in `ConsumeResult` will be moved to
    /// caller
    virtual ConsumeResult consume(UInt32 count, Int32 timeout_ms, std::any & ctx) = 0;

    /// return nonzero if failed, otherwise return 0
    virtual Int32 stopConsume(std::any & ctx) = 0;

    /// Move the consuming sequence numbers forward
    /// return non-zero if failed, otherwise return zero
    virtual Int32 commit(RecordSN sn, std::any & ctx) = 0;

    /// Admin APIs
    /// `create` creates a WAL log named `name`. Returns 0 if success; otherwise non-zero
    virtual Int32 create(const String & name, std::any & ctx) = 0;

    /// `remove` deletes a WAL log named `name`. Returns 0 if success; otherwise non-zero
    virtual Int32 remove(const String & name, std::any & ctx) = 0;

    /// `describe` return 0 if success; otherwise return non-zero
    virtual Int32 describe(const String & name, std::any & ctx) const = 0;

    virtual ClusterPtr cluster(std::any & ctx) const = 0;
};

using WALPtr = std::shared_ptr<WAL>;
}
