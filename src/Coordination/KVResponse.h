#pragma once

#include <Coordination/KVRequest.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
extern const int OK;
extern const int LOGICAL_ERROR;
extern const int RAFT_ERROR;
}

namespace Coordination
{
using namespace DB;

struct KVResponse : public TypePromotion<KVResponse>
{
    int32_t code = ErrorCodes::OK;
    String msg;
    KVString column_family;

    KVResponse() = default;
    KVResponse(const KVResponse &) = default;
    KVResponse(int32_t err, const String & err_msg) : code(err), msg(err_msg) { }
    virtual ~KVResponse() = default;

    virtual KVOpNum getOpNum() const = 0;

    /// Writes length, xid, op_num, then the rest.
    void write(WriteBuffer & out);

    static std::shared_ptr<KVResponse> read(ReadBuffer & in);

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void readImpl(ReadBuffer &) = 0;
};

using KVResponsePtr = std::shared_ptr<KVResponse>;

struct KVEmptyResponse final : public KVResponse
{
    KVEmptyResponse() : KVResponse(ErrorCodes::RAFT_ERROR, "empty repsonse") { }

    KVOpNum getOpNum() const override { return KVOpNum::UNKNOWN; }
    inline void writeImpl(WriteBuffer & /*out*/) const override { }
    inline void readImpl(ReadBuffer & /*in*/) override { }
};

struct KVMultiGetResponse final : public KVResponse
{
    KVStringPairs kv_pairs;

    KVOpNum getOpNum() const override { return KVOpNum::MULTIGET; }
    inline void writeImpl(WriteBuffer & out) const override { writeBinary(kv_pairs, out); }
    inline void readImpl(ReadBuffer & in) override { readBinary(kv_pairs, in); }
};

struct KVMultiPutResponse final : public KVResponse
{
    KVOpNum getOpNum() const override { return KVOpNum::MULTIPUT; }

    inline void writeImpl(WriteBuffer & /*out*/) const override { }
    inline void readImpl(ReadBuffer & /*in*/) override { }
};

struct KVMultiDeleteResponse final : public KVResponse
{
    KVOpNum getOpNum() const override { return KVOpNum::MULTIDELETE; }

    inline void writeImpl(WriteBuffer & /*out*/) const override { }
    inline void readImpl(ReadBuffer & /*in*/) override { }
};

struct KVListResponse final : public KVResponse
{
    KVStringPairs kv_pairs;

    KVOpNum getOpNum() const override { return KVOpNum::LIST; }
    inline void writeImpl(WriteBuffer & out) const override { writeBinary(kv_pairs, out); }
    inline void readImpl(ReadBuffer & in) override { readBinary(kv_pairs, in); }
};

class KVResponseFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<KVResponsePtr()>;
    using KVOpNumToResponse = std::unordered_map<KVOpNum, Creator>;

    static KVResponseFactory & instance()
    {
        static KVResponseFactory factory;
        return factory;
    }

    KVResponsePtr get(KVOpNum op_num)
    {
        auto it = op_num_to_response.find(op_num);
        if (it == op_num_to_response.end())
            throw DB::Exception("Unknown operation type " + toString(op_num), ErrorCodes::LOGICAL_ERROR);

        return it->second();
    }

    KVResponsePtr get(KVRequestPtr request)
    {
        auto it = op_num_to_response.find(request->getOpNum());
        if (it == op_num_to_response.end())
            throw DB::Exception("Unknown operation type " + toString(request->getOpNum()), ErrorCodes::LOGICAL_ERROR);

        it->second()->column_family = request->column_family;
        return it->second();
    }

    void registerResponse(KVOpNum op_num, Creator creator)
    {
        if (!op_num_to_response.try_emplace(op_num, creator).second)
            throw DB::Exception("Response type " + toString(op_num) + " already registered", ErrorCodes::LOGICAL_ERROR);
    }

private:
    KVOpNumToResponse op_num_to_response;

private:
    KVResponseFactory();
};
}
