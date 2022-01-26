#include "KVResponse.h"

namespace Coordination
{
using namespace DB;

//namespace
//{
//    KVOpNum getKVOpNum(int32_t raw_op_num)
//    {
//        //    if (!VALID_OPERATIONS.count(raw_op_num))
//        //        throw Exception("Operation " + std::to_string(raw_op_num) + " is unknown", Error::ZUNIMPLEMENTED);
//        return static_cast<KVOpNum>(raw_op_num);
//    }
//}

void KVResponse::write(WriteBuffer & out)
{
    writeIntBinary<uint32_t>(static_cast<int32_t>(getOpNum()), out);
    writeIntBinary<int32_t>(code, out);
    writeStringBinary(msg, out);
    writeBinary(column_family, out);
    writeImpl(out);
    out.next();
}

std::shared_ptr<KVResponse> KVResponse::read(ReadBuffer & in)
{
    KVOpNum op_num;
    int32_t code;
    String msg;
    KVString column_family;
    readIntBinary<uint32_t>(reinterpret_cast<uint32_t &>(op_num), in);
    readIntBinary<int32_t>(code, in);
    readStringBinary(msg, in);
    readBinary(column_family, in);

    auto response = KVResponseFactory::instance().get(op_num);
    response->code = code;
    response->msg = msg;
    response->column_family = column_family;
    response->readImpl(in);
    return response;
}

template <KVOpNum num, typename ResponseT>
void registerKVResponse(KVResponseFactory & factory)
{
    factory.registerResponse(num, [] { return std::make_shared<ResponseT>(); });
}

KVResponseFactory::KVResponseFactory()
{
    registerKVResponse<KVOpNum::MULTIGET, KVMultiGetResponse>(*this);
    registerKVResponse<KVOpNum::MULTIPUT, KVMultiPutResponse>(*this);
    registerKVResponse<KVOpNum::MULTIDELETE, KVMultiDeleteResponse>(*this);
    registerKVResponse<KVOpNum::LIST, KVListResponse>(*this);
}
}
