#pragma once

#include <Common/TypePromotion.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Coordination
{
using namespace DB;

enum class KVOpNum : uint32_t
{
    UNKNOWN = 0,
    PUT = 1,
    MULTIPUT = 2,
    GET = 3,
    MULTIGET = 4,
    DELETE = 5,
    MULTIDELETE = 6
};
namespace
{
    std::string toString(KVOpNum op_num)
    {
        switch (op_num)
        {
            case KVOpNum::UNKNOWN:
                return "Unknown";
            case KVOpNum::PUT:
                return "Put";
            case KVOpNum::MULTIPUT:
                return "MultiPut";
            case KVOpNum::GET:
                return "Get";
            case KVOpNum::MULTIGET:
                return "MultiGet";
            case KVOpNum::DELETE:
                return "Delete";
            case KVOpNum::MULTIDELETE:
                return "MultiDelete";
        }
        int32_t raw_op = static_cast<int32_t>(op_num);
        throw DB::Exception("Operation " + std::to_string(raw_op) + " is unknown", DB::ErrorCodes::LOGICAL_ERROR);
    }
}

struct KVRequest: public TypePromotion<KVRequest>
{
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

struct KVGetRequest final : public KVRequest
{
    String key;

    KVOpNum getOpNum() const override { return KVOpNum::GET; }
    inline void writeImpl(WriteBuffer & out) const override { writeStringBinary(key, out); }

    inline void readImpl(ReadBuffer & in) override { readStringBinary(key, in); }

    bool isReadRequest() const override { return true; }
};

struct KVMultiGetRequest final : public KVRequest
{
    std::vector<String> keys;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIGET; }
    inline void writeImpl(WriteBuffer & out) const override { writeVectorBinary<String>(keys, out); }

    inline void readImpl(ReadBuffer & in) override { readVectorBinary<String>(keys, in); }

    bool isReadRequest() const override { return true; }
};

struct KVPutRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    String key;
    String value;

    KVOpNum getOpNum() const override { return KVOpNum::PUT; }

    inline void writeImpl(WriteBuffer & out) const override
    {
        writeStringBinary(key, out);
        writeStringBinary(value, out);
    }

    inline void readImpl(ReadBuffer & in) override
    {
        readStringBinary(key, in);
        readStringBinary(value, in);
    }

    bool isReadRequest() const override { return false; }
};

struct KVMultiPutRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    std::vector<String> keys;
    std::vector<String> values;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIPUT; }

    inline void writeImpl(WriteBuffer & out) const override
    {
        writeVectorBinary<String>(keys, out);
        writeVectorBinary<String>(values, out);
    }

    inline void readImpl(ReadBuffer & in) override
    {
        readVectorBinary<String>(keys, in);
        readVectorBinary<String>(values, in);
    }

    bool isReadRequest() const override { return false; }
};

struct KVDeleteRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    String key;

    KVOpNum getOpNum() const override { return KVOpNum::DELETE; }

    inline void writeImpl(WriteBuffer & out) const override
    {
        writeStringBinary(key, out);
    }

    inline void readImpl(ReadBuffer & in) override
    {
        readStringBinary(key, in);
    }

    bool isReadRequest() const override { return false; }
};

struct KVMultiDeleteRequest final : public KVRequest
{
    // TODO: use Slice in future to avoid memory copy
    std::vector<String> keys;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIDELETE; }

    inline void writeImpl(WriteBuffer & out) const override
    {
        writeVectorBinary<String>(keys, out);
    }

    inline void readImpl(ReadBuffer & in) override
    {
        readVectorBinary<String>(keys, in);
    }

    bool isReadRequest() const override { return false; }
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
