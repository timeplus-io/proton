#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/TypePromotion.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Coordination
{
using namespace DB;

using KVString = String;
using KVStrings = std::vector<KVString>;
using KVStringPair = std::pair<KVString, KVString>;
using KVStringPairs = std::vector<KVStringPair>;

enum class KVOpNum : uint32_t
{
    UNKNOWN = 0,
    MULTIPUT,
    MULTIGET,
    MULTIDELETE,
    LIST
};
namespace
{
    std::string toString(KVOpNum op_num)
    {
        switch (op_num)
        {
            case KVOpNum::UNKNOWN:
                return "Unknown";
            case KVOpNum::MULTIPUT:
                return "MultiPut";
            case KVOpNum::MULTIGET:
                return "MultiGet";
            case KVOpNum::MULTIDELETE:
                return "MultiDelete";
            case KVOpNum::LIST:
                return "List";
        }
        int32_t raw_op = static_cast<int32_t>(op_num);
        throw DB::Exception("Operation " + std::to_string(raw_op) + " is unknown", DB::ErrorCodes::LOGICAL_ERROR);
    }
}

struct KVRequest : public TypePromotion<KVRequest>
{
    KVString column_family;

    KVRequest() = default;
    KVRequest(const KVRequest &) = default;
    virtual ~KVRequest() = default;

    virtual KVOpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out);

    static std::shared_ptr<KVRequest> read(ReadBuffer & in);

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;

    virtual bool isReadRequest() const = 0;
};

using KVRequestPtr = std::shared_ptr<KVRequest>;

struct KVMultiGetRequest final : public KVRequest
{
    KVStrings keys;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIGET; }
    inline void writeImpl(WriteBuffer & out) const override { writeBinary(keys, out); }

    inline void readImpl(ReadBuffer & in) override { readBinary(keys, in); }

    bool isReadRequest() const override { return true; }
};

struct KVMultiPutRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    KVStringPairs kv_pairs;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIPUT; }

    inline void writeImpl(WriteBuffer & out) const override { writeBinary(kv_pairs, out); }

    inline void readImpl(ReadBuffer & in) override { readBinary(kv_pairs, in); }

    bool isReadRequest() const override { return false; }
};

struct KVMultiDeleteRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    KVStrings keys;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIDELETE; }

    inline void writeImpl(WriteBuffer & out) const override { writeBinary(keys, out); }

    inline void readImpl(ReadBuffer & in) override { readBinary(keys, in); }

    bool isReadRequest() const override { return false; }
};

struct KVListRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    KVString key_prefix;

    KVOpNum getOpNum() const override { return KVOpNum::LIST; }

    inline void writeImpl(WriteBuffer & out) const override { writeBinary(key_prefix, out); }

    inline void readImpl(ReadBuffer & in) override { readBinary(key_prefix, in); }

    bool isReadRequest() const override { return true; }
};

class KVRequestFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<KVRequestPtr()>;
    using KVOpNumToRequest = std::unordered_map<KVOpNum, Creator>;

    static KVRequestFactory & instance()
    {
        static KVRequestFactory factory;
        return factory;
    }

    KVRequestPtr get(KVOpNum op_num)
    {
        auto it = op_num_to_request.find(op_num);
        if (it == op_num_to_request.end())
            throw DB::Exception("Unknown operation type " + toString(op_num), ErrorCodes::LOGICAL_ERROR);

        return it->second();
    }

    void registerRequest(KVOpNum op_num, Creator creator)
    {
        if (!op_num_to_request.try_emplace(op_num, creator).second)
            throw DB::Exception("Request type " + toString(op_num) + " already registered", ErrorCodes::LOGICAL_ERROR);
    }

private:
    KVOpNumToRequest op_num_to_request;

private:
    KVRequestFactory();
};
}
