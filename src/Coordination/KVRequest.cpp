#include "KVRequest.h"

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

void KVRequest::write(WriteBuffer & out)
{
    writeIntBinary<uint32_t>(static_cast<int32_t>(getOpNum()), out);
    writeBinary(column_family, out);
    writeImpl(out);
    out.next();
}

std::shared_ptr<KVRequest> KVRequest::read(ReadBuffer & in)
{
    KVOpNum op_num;
    KVString column_family;
    readIntBinary<uint32_t>(reinterpret_cast<uint32_t &>(op_num), in);
    readBinary(column_family, in);

    auto request = KVRequestFactory::instance().get(op_num);
    request->column_family = column_family;
    request->readImpl(in);
    return request;
}

template <KVOpNum num, typename RequestT>
void registerKVRequest(KVRequestFactory & factory)
{
    factory.registerRequest(num, [] { return std::make_shared<RequestT>(); });
}

KVRequestFactory::KVRequestFactory()
{
    registerKVRequest<KVOpNum::MULTIGET, KVMultiGetRequest>(*this);
    registerKVRequest<KVOpNum::MULTIPUT, KVMultiPutRequest>(*this);
    registerKVRequest<KVOpNum::MULTIDELETE, KVMultiDeleteRequest>(*this);
    registerKVRequest<KVOpNum::LIST, KVListRequest>(*this);
}
}
