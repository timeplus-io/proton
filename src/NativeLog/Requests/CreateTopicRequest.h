#pragma once

namespace nlog
{
struct CreateTopicRequest
{
    std::string name;
    uint32_t partitions;
    uint32_t replicas;
    bool compacted;
};
}
