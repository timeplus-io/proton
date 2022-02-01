#pragma once

#include <Poco/UUID.h>
#include <fmt/format.h>

namespace nlog
{
struct TopicInfo
{
    Poco::UUID id; /// UUID
    std::string ns;
    std::string name;
    int32_t partitions;
    int32_t replicas;
    int32_t version;
    bool compacted;
    int64_t create_timestamp;
    int64_t last_modify_timestamp;

    std::string string() const
    {
        return fmt::format(
            "id={} namespace={} name={} partitions={} replicas={} compacted={} version={} create_timestamp={} last_modify_timestamp={}",
            id.toString(),
            ns,
            name,
            partitions,
            replicas,
            compacted,
            version,
            create_timestamp,
            last_modify_timestamp);
    }
};
}
