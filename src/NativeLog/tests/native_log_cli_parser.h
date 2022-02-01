#pragma once

#include <optional>
#include <string>

namespace nlog
{
struct TopicArgs
{
    std::string command = "list";
    std::string ns;
    std::string name;
    uint32_t partitions;
    bool compacted;
};

struct ProduceArgs
{
    std::string ns;
    std::string topic;
    int64_t num_records;
    int64_t record_size;
    int64_t record_batch_size;
    int64_t concurrency;
    bool validate_offsets = false;
};

struct ConsumeArgs
{
    std::string ns;
    std::string topic;
    /// earliest: -2
    /// latest: -1
    int64_t start_offset;
    int64_t num_records;
    int64_t buf_size;
    bool single_thread = false;
    bool validate_offsets = false;
};

struct TrimArgs
{
    std::string ns;
    std::string topic;
    int64_t to_offset;
};

struct NativeLogArgs
{
    std::string log_root_directory;
    std::string meta_root_directory;

    std::string command = "consume";

    std::optional<TopicArgs> topic_args;
    std::optional<ProduceArgs> produce_args;
    std::optional<ConsumeArgs> consume_args;
    std::optional<TrimArgs> trim_args;

    bool valid() const
    {
        if (command == "consume")
            return consume_args.has_value();
        else if (command == "produce")
            return produce_args.has_value();
        else if (command == "topic")
            return topic_args.has_value();
        else if (command == "trim")
            return trim_args.has_value();
        else
            return false;
    }
};

NativeLogArgs parseArgs(int argc, char ** argv);
}
