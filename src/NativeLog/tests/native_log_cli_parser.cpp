#include "native_log_cli_parser.h"

#include <Common/Exception.h>
#include <Common/TerminalSize.h>

#include <boost/program_options.hpp>

#include <iostream>

namespace nlog
{
namespace
{
    void printUsage(const char * prog, const po::options_description & desc) { std::cerr << "Usage: " << prog << " " << desc << std::endl; }

    NativeLogArgs parseTopicListArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("list options", getTerminalWidth());

        /// native_log topic list [--namespace <namespace> [--name <topic-name>]]
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>()->default_value(""), "namespace of the topic");
        options("name", value<std::string>()->default_value(""), "topic name");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse list args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        TopicArgs args;
        args.command = "list";
        args.ns = option_map["namespace"].as<std::string>();
        args.name = option_map["name"].as<std::string>();

        if (!args.name.empty() && args.ns.empty())
        {
            std::cerr << "namespace is required when topic name is specified\n";
            printUsage(progname, desc);
            return {};
        }

        NativeLogArgs nl_args;
        nl_args.command = "topic";
        nl_args.topic_args = args;
        return nl_args;
    }

    NativeLogArgs parseTopicCreateArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("create options", getTerminalWidth());

        /// native_log topic delete --namespace <namespace> --name <topic-name>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the topic");
        options("name", value<std::string>(), "topic name");
        options("partitions", value<uint32_t>()->default_value(1), "number of partitions");
        /// options("replication_factor", value<uint32_t>()->default_value(1), "number of replicas per partition");
        options("compact", value<bool>()->default_value(false), "Compact topic or not");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse create args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        /// Non empty values for keys
        for (const auto & key : {"namespace", "name"})
        {
            if (!option_map.contains(key) || option_map[key].as<std::string>().empty())
            {
                std::cerr << "Missing or having empty value for --" << key << std::endl;
                printUsage(progname, desc);
                return {};
            }
        }

        TopicArgs args;
        args.command = "create";
        args.ns = option_map["namespace"].as<std::string>();
        args.name = option_map["name"].as<std::string>();
        args.partitions = option_map["partitions"].as<uint32_t>();
        args.compacted = option_map["compact"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "topic";
        nl_args.topic_args = args;
        return nl_args;
    }

    NativeLogArgs parseTopicDeleteArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("create options", getTerminalWidth());

        /// native_log topic delete --namespace <namespace> --name <topic-name>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the topic");
        options("name", value<std::string>(), "topic name");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse delete args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        /// Non empty values for keys
        for (const auto & key : {"namespace", "name"})
        {
            if (!option_map.contains(key) || option_map[key].as<std::string>().empty())
            {
                std::cerr << "Missing or having empty value for --" << key << std::endl;
                printUsage(progname, desc);
                return {};
            }
        }

        TopicArgs args;
        args.command = "delete";
        args.ns = option_map["namespace"].as<std::string>();
        args.name = option_map["name"].as<std::string>();

        NativeLogArgs nl_args;
        nl_args.command = "topic";
        nl_args.topic_args = args;
        return nl_args;
    }

    NativeLogArgs parseTopicArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("topic options", getTerminalWidth());
        auto cmds = desc.add_options();
        cmds("subcommand", value<std::string>()->default_value("help"), "<list|create|delete> subcommand to execute");
        cmds("subargs", value<std::vector<std::string>>(), "Arguments for subcommand of topic");

        po::positional_options_description pos;
        pos.add("subcommand", 1).add("subargs", -1);

        /// Collect all the unrecognized options from the first pass. This will include the first positional
        /// command name, so we need erase that
        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse again
        po::variables_map option_map;
        po::parsed_options topic_subcmd_parsed = po::command_line_parser(opts).options(desc).positional(pos).allow_unregistered().run();
        po::store(topic_subcmd_parsed, option_map);

        std::string topic_subcmd = option_map["subcommand"].as<std::string>();
        if (topic_subcmd == "list")
        {
            return parseTopicListArgs(topic_subcmd_parsed, progname);
        }
        else if (topic_subcmd == "create")
        {
            return parseTopicCreateArgs(topic_subcmd_parsed, progname);
        }
        else if (topic_subcmd == "delete")
        {
            return parseTopicDeleteArgs(topic_subcmd_parsed, progname);
        }
        else
        {
            printUsage(progname, desc);
            return {};
        }
    }

    NativeLogArgs parseProduceArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("produce options", getTerminalWidth());

        /// native_log produce --namespace <namespace> --topic <topic-name> --num_records <total-records>
        /// --batch_size <batch_size> --record_size <record_size> --concurrency <concurrency>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the topic");
        options("topic", value<std::string>(), "topic name");
        options("num_records", value<int64_t>()->default_value(1000000), "total number of records");
        options("record_size", value<int64_t>()->default_value(100), "Single record size");
        options("record_batch_size", value<int64_t>()->default_value(100), "Number of records in one batch");
        options(
            "concurrency",
            value<int64_t>()->default_value(1),
            "Number of produce threads. Each thread will have a fair share of the total records");
        options("validate_offsets", value<bool>()->default_value(false), "if validate record offsets");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse produce args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        /// Non empty values for keys
        for (const auto & key : {"namespace", "topic"})
        {
            if (!option_map.contains(key) || option_map[key].as<std::string>().empty())
            {
                std::cerr << "Missing or having empty value for --" << key << std::endl;
                printUsage(progname, desc);
                return {};
            }
        }

        ProduceArgs args;
        args.ns = option_map["namespace"].as<std::string>();
        args.topic = option_map["topic"].as<std::string>();
        args.num_records = option_map["num_records"].as<int64_t>();
        args.record_size = option_map["record_size"].as<int64_t>();
        args.record_batch_size = option_map["record_batch_size"].as<int64_t>();
        args.concurrency = option_map["concurrency"].as<int64_t>();
        args.validate_offsets = option_map["validate_offsets"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "produce";
        nl_args.produce_args = args;
        return nl_args;
    }

    NativeLogArgs parseConsumeArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("consume options", getTerminalWidth());

        /// native_log consume --namespace <namespace> --topic <topic-name> --num_records <total-records>
        /// --start_offset [-1|-2|offset] --concurrency <concurrency>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the topic");
        options("topic", value<std::string>(), "topic name");
        options("start_offset", value<int64_t>()->default_value(-1), "start offset to consume from");
        options("num_records", value<int64_t>()->default_value(1000000), "total number of records");
        options("buf_size", value<int64_t>()->default_value(50*1024*1024), "buffer size to read records");
        options("single_thread", value<bool>()->default_value(false), "Use single thread to consume. Otherwise one thread per partition");
        options("validate_offsets", value<bool>()->default_value(false), "if validate record offsets");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse produce args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        /// Non empty values for keys
        for (const auto & key : {"namespace", "topic"})
        {
            if (!option_map.contains(key) || option_map[key].as<std::string>().empty())
            {
                std::cerr << "Missing or having empty value for --" << key << std::endl;
                printUsage(progname, desc);
                return {};
            }
        }

        ConsumeArgs args;
        args.ns = option_map["namespace"].as<std::string>();
        args.topic = option_map["topic"].as<std::string>();
        args.start_offset = option_map["start_offset"].as<int64_t>();
        args.num_records = option_map["num_records"].as<int64_t>();
        args.buf_size = option_map["buf_size"].as<int64_t>();
        args.single_thread = option_map["single_thread"].as<bool>();
        args.validate_offsets = option_map["validate_offsets"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "consume";
        nl_args.consume_args = args;
        return nl_args;
    }

    NativeLogArgs parseTrimArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("consume options", getTerminalWidth());

        /// native_log trim --namespace <namespace> --topic <topic-name> --to_offset [offset]
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the topic");
        options("topic", value<std::string>(), "topic name");
        options("to_offset", value<int64_t>()->default_value(0), "Offset to trim to");

        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse produce args
        po::variables_map option_map;
        try
        {
            po::store(po::command_line_parser(opts).options(desc).run(), option_map);
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(false, true) << std::endl;
            printUsage(progname, desc);
            return {};
        }

        if (option_map.contains("help"))
        {
            printUsage(progname, desc);
            return {};
        }

        /// Non empty values for keys
        for (const auto & key : {"namespace", "topic"})
        {
            if (!option_map.contains(key) || option_map[key].as<std::string>().empty())
            {
                std::cerr << "Missing or having empty value for --" << key << std::endl;
                printUsage(progname, desc);
                return {};
            }
        }

        TrimArgs args;
        args.ns = option_map["namespace"].as<std::string>();
        args.topic = option_map["topic"].as<std::string>();
        args.to_offset = option_map["to_offset"].as<int64_t>();

        NativeLogArgs nl_args;
        nl_args.command = "trim";
        nl_args.trim_args = args;
        return nl_args;
    }
}

NativeLogArgs parseArgs(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description global = createOptionsDescription("Global options", getTerminalWidth());
    auto cmds = global.add_options();
    cmds("command", value<std::string>()->default_value("help"), "<topic|produce|consume|trim> subcommand to execute");
    cmds("subargs", value<std::vector<std::string>>(), "Arguments for subcommand");

    po::positional_options_description pos;
    pos.add("command", 1).add("subargs", -1);

    po::variables_map option_map;
    po::parsed_options cmd_parsed = po::command_line_parser(argc, argv).options(global).positional(pos).allow_unregistered().run();
    po::store(cmd_parsed, option_map);

    String cmd = option_map["command"].as<String>();
    if (cmd == "produce")
    {
        return parseProduceArgs(cmd_parsed, argv[0]);
    }
    else if (cmd == "consume")
    {
        return parseConsumeArgs(cmd_parsed, argv[0]);
    }
    else if (cmd == "topic")
    {
        return parseTopicArgs(cmd_parsed, argv[0]);
    }
    else if (cmd == "trim")
    {
        return parseTrimArgs(cmd_parsed, argv[0]);
    }
    else
    {
        printUsage(argv[0], global);
        return {};
    }
}
}
