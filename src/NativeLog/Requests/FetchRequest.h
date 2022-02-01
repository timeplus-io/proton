#pragma once

namespace nlog
{
struct FetchRequest
{
    struct TopicPartitionOffset
    {
        TopicPartitionOffset(const std::string & topic_, int32_t partition_, int64_t offset_, int64_t max_size_ = 4 * 1024 * 1024)
            : topic(topic_), partition(partition_), offset(offset_), max_size(max_size_)
        {
        }

        std::string topic;
        int32_t partition;
        /// -1 latest
        /// -2 earliest
        int64_t offset;
        int64_t max_size;
    };

    std::vector<TopicPartitionOffset> offsets;
};
}
