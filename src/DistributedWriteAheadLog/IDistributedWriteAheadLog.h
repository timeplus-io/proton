#pragma once

#include "ByteVector.h"

#include <Core/Block.h>

#include <any>


namespace DB
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

class IDistributedWriteAheadLog : private boost::noncopyable
{
public:
    virtual ~IDistributedWriteAheadLog() = default;
    virtual void startup();
    virtual void shutdown();

    enum class OpCode : UInt8
    {
        /// Data
        ADD_DATA_BLOCK = 0,
        ALTER_DATA_BLOCK,

        /// Table Metadata
        CREATE_TABLE,
        DELETE_TABLE,
        ALTER_TABLE,

        /// Database
        CREATE_DATABASE,
        DELETE_DATABASE,

        /// Dictionary
        CREATE_DICTIONARY,
        DELETE_DICTIONARY,

        MAX_OPS_CODE,

        UNKNOWN = 0x3F,
    };

    using RecordSequenceNumber = Int64;

    struct Record;

    using Records = std::vector<Record>;
    using RecordPtr = std::shared_ptr<Record>;
    using RecordPtrs = std::vector<RecordPtr>;

    inline const static String IDEMPOTENT_KEY = "_idem";

    struct Record
    {
        /// fields on the wire
        OpCode op_code = OpCode::UNKNOWN;

        std::unordered_map<String, String> headers;

        Block block;

        /// fields which are not on the wire
        UInt64 partition_key = 0;

        RecordSequenceNumber sn = -1;

        bool empty() const { return block.rows() == 0; }

        bool hasIdempotentKey() const { return headers.contains(IDEMPOTENT_KEY) && !headers.at(IDEMPOTENT_KEY).empty(); }
        String & idempotentKey() { return headers.at(IDEMPOTENT_KEY); }
        void setIdempotentKey(const String & key) { headers[IDEMPOTENT_KEY] = key; }

        static UInt8 ALWAYS_INLINE version(UInt64 flags)
        {
            return flags & 0x1F;
        }

        static OpCode ALWAYS_INLINE opcode(UInt64 flags)
        {
            auto opcode = (flags >> 5ul) & 0x3F;
            if (likely(opcode < static_cast<UInt64>(OpCode::MAX_OPS_CODE)))
            {
                return static_cast<OpCode>(opcode);
            }
            return OpCode::UNKNOWN;
        }

        static ByteVector write(const Record & record);
        static RecordPtr read(const char * data, size_t size);

        Record(OpCode op_code_, Block && block_) : op_code(op_code_), block(std::move(block_)) { }
    };

    constexpr static UInt8 WAL_VERSION = 1;

    struct AppendResult
    {
        RecordSequenceNumber sn = -1;

        /// 0 if success, otherwise non-zero
        /// FIXME, mapping to ErrorCodes
        Int32 err = 0;
        std::any ctx;
    };

    struct ConsumeResult
    {
        Int32 err = 0;
        RecordPtrs records;
    };

    using AppendCallback = void (*)(const AppendResult & result, void * data);
    using ConsumeCallback = void (*)(RecordPtrs records, void * data);

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
    virtual Int32 commit(RecordSequenceNumber sequence_number, std::any & ctx) = 0;

    /// Admin APIs
    /// `create` creates a WAL log named `name`. Returns 0 if success; otherwise non-zero
    virtual Int32 create(const String & name, std::any & ctx) = 0;

    /// `remove` deltes a WAL log named `name`. Returns 0 if success; otherwise non-zero
    virtual Int32 remove(const String & name, std::any & ctx) = 0;

    /// `describe` return 0 if success; otherwise return non-zero
    virtual Int32 describe(const String & name, std::any & ctx) = 0;
};

using DistributedWriteAheadLogPtr = std::shared_ptr<IDistributedWriteAheadLog>;
}
