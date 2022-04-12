#pragma once

#include <optional>
#include <string>

namespace nlog
{
struct StreamArgs
{
    std::string command = "list";
    std::string ns;
    std::string stream;
    int32_t shards;
    bool compacted;
};

struct ProduceArgs
{
    std::string ns;
    std::string stream;
    int64_t num_records;
    int64_t record_batch_size;
    int64_t concurrency;
    bool validate_sns = false;
};

struct ConsumeArgs
{
    std::string ns;
    std::string stream;
    /// earliest: -2
    /// latest: -1
    int64_t start_sn;
    int64_t num_records;
    int64_t buf_size;
    bool single_thread = false;
    bool validate_sns = false;
};

struct TrimArgs
{
    std::string ns;
    std::string stream;
    int64_t to_sn;
};

struct NativeLogArgs
{
    std::string log_root_directory;
    std::string meta_root_directory;

    std::string command = "consume";

    std::optional<StreamArgs> stream_args;
    std::optional<ProduceArgs> produce_args;
    std::optional<ConsumeArgs> consume_args;
    std::optional<TrimArgs> trim_args;

    bool valid() const
    {
        if (command == "consume")
            return consume_args.has_value();
        else if (command == "produce")
            return produce_args.has_value();
        else if (command == "stream")
            return stream_args.has_value();
        else if (command == "trim")
            return trim_args.has_value();
        else
            return false;
    }
};

NativeLogArgs parseArgs(int argc, char ** argv);
}
