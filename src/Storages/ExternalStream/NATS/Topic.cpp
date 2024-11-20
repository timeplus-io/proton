#include <nats/nats.h>
#include <Storages/ExternalStream/NATS/Topic.h>

namespace DB
{

namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

namespace NATS
{

Topic::Topic(natsConnection * conn, const std::string & subject) : conn(conn), subject(subject)
{
    natsStatus s = natsConnection_SubscribeSync(&sub, conn, subject.c_str());
    if (s != NATS_OK)
    {
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Failed to subscribe to subject {}, error={}", subject, natsStatus_GetText(s));
    }
}

int Topic::getPartitionCount() const
{
    // NATS does not have partitions, return 1
    return 1;
}

WatermarkOffsets Topic::queryWatermarks() const
{
    // NATS does not have watermarks, return default values
    return {0, 0};
}

}

}
