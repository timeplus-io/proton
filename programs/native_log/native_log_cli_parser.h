#pragma once

#include <optional>
#include <string>

namespace cluster::nlog
{
enum class Command : uint8_t
{
    None,
    Produce,
    Consume,
    Stream,
    Trim,
    DumpIndex,
};

struct StreamArgs
{
    enum class SubCommand : uint8_t
    {
        List,
        Create,
        Delete,
        Rename,
    };
    SubCommand subcommand = SubCommand::List;

    std::string ns;
    std::string stream;
    std::string new_stream;
    int32_t shards;
    bool compacted;
};

struct ProduceArgs
{
    uint64_t num_records;
    int64_t event_size;
    int64_t batch_size;
    bool validate_sns = false;
};

struct ConsumeArgs
{
    /// earliest: -2
    /// latest: -1
    int64_t start_sn;
    uint64_t num_records;
    int64_t buf_size;
    bool print_each_entry = false;
    bool single_thread = false;
    bool validate_sns = false;
};

struct TrimArgs
{
    int64_t start_sn;
};

struct DumpIndexArgs
{
    enum class IndexType : uint8_t
    {
        SequencePosition,
        TimestampSequence,
        EpochSequence
    };

    IndexType index_type = IndexType::EpochSequence;
};

struct NativeLogArgs
{
    std::string log_root_directory;

    Command command = Command::Consume;

    std::optional<StreamArgs> stream_args;
    std::optional<ProduceArgs> produce_args;
    std::optional<ConsumeArgs> consume_args;
    std::optional<TrimArgs> trim_args;
    std::optional<DumpIndexArgs> dump_index_args;

    bool valid() const
    {
        if (log_root_directory.empty())
            return false;

        switch (command)
        {
            case Command::Consume:
                return consume_args.has_value();
            case Command::Produce:
                return produce_args.has_value();
            case Command::Stream:
                return stream_args.has_value();
            case Command::Trim:
                return trim_args.has_value();
            case Command::DumpIndex:
                return dump_index_args.has_value();
            default:
                return false;
        }
    }
};

NativeLogArgs parseArgs(int argc, char ** argv);
}
