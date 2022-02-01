#pragma once

#include <NativeLog/Schemas/Record.h>

namespace nlog
{
struct ProduceRequest
{
    ProduceRequest(const std::string & topic_, uint32_t partition_, MemoryRecords batch_)
        : topic(topic_), partition(partition_), batch(std::move(batch_))
    {
    }

    std::string topic;
    uint32_t partition;
    MemoryRecords batch;
};
}
