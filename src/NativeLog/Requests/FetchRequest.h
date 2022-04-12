#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/StreamShard.h>

#include <vector>

namespace nlog
{
/// Fetch data from a list of shards of (different) streams
struct FetchRequest : public CommonRequest
{
public:
    struct FetchDescription
    {
        FetchDescription(std::string stream_, const StreamID & stream_id, int32_t shard_, int64_t start_sn_, int64_t max_wait_ms_ = 500, int64_t fetch_max_size_ = 8 * 1024 * 1024)
            : stream_shard(std::move(stream_), stream_id, shard_), sn(start_sn_), max_wait_ms(max_wait_ms_), max_size(fetch_max_size_)
        {
        }

        StreamShard stream_shard;
        /// -1 latest
        /// -2 earliest
        int64_t sn;
        int64_t position = -1;
        int64_t max_wait_ms;
        int64_t max_size;
    };

public:
    explicit FetchRequest(std::vector<FetchDescription> fetch_descs_, int32_t api_version_ = 0)
        : CommonRequest(api_version_), fetch_descs(std::move(fetch_descs_))
    {
    }

    std::vector<FetchDescription> fetch_descs;
};
}
