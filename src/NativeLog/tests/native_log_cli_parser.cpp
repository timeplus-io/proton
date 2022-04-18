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

    NativeLogArgs parseStreamListArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("list options", getTerminalWidth());

        /// native_log stream list [--namespace <namespace> [--name <stream-name>]]
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>()->default_value(""), "namespace of the stream");
        options("name", value<std::string>()->default_value(""), "stream name");

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

        StreamArgs args;
        args.command = "list";
        args.ns = option_map["namespace"].as<std::string>();
        args.stream = option_map["name"].as<std::string>();

        if (!args.stream.empty() && args.ns.empty())
        {
            std::cerr << "namespace is required when stream name is specified\n";
            printUsage(progname, desc);
            return {};
        }

        NativeLogArgs nl_args;
        nl_args.command = "stream";
        nl_args.stream_args = args;
        return nl_args;
    }

    NativeLogArgs parseStreamCreateArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("create options", getTerminalWidth());

        /// native_log stream delete --namespace <namespace> --name <stream-name>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the stream");
        options("name", value<std::string>(), "stream name");
        options("shards", value<uint32_t>()->default_value(1), "number of shards");
        /// options("replication_factor", value<uint32_t>()->default_value(1), "number of replicas per partition");
        options("compact", value<bool>()->default_value(false), "Compact stream or not");

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

        StreamArgs args;
        args.command = "create";
        args.ns = option_map["namespace"].as<std::string>();
        args.stream = option_map["name"].as<std::string>();
        args.shards = option_map["shards"].as<uint32_t>();
        args.compacted = option_map["compact"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "stream";
        nl_args.stream_args = args;
        return nl_args;
    }

    NativeLogArgs parseStreamDeleteArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("create options", getTerminalWidth());

        /// native_log stream delete --namespace <namespace> --name <stream-name>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the stream");
        options("name", value<std::string>(), "stream name");

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

        StreamArgs args;
        args.command = "delete";
        args.ns = option_map["namespace"].as<std::string>();
        args.stream = option_map["name"].as<std::string>();

        NativeLogArgs nl_args;
        nl_args.command = "stream";
        nl_args.stream_args = args;
        return nl_args;
    }

    NativeLogArgs parseStreamArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("stream options", getTerminalWidth());
        auto cmds = desc.add_options();
        cmds("subcommand", value<std::string>()->default_value("help"), "<list|create|delete> subcommand to execute");
        cmds("subargs", value<std::vector<std::string>>(), "Arguments for subcommand of stream");

        po::positional_options_description pos;
        pos.add("subcommand", 1).add("subargs", -1);

        /// Collect all the unrecognized options from the first pass. This will include the first positional
        /// command name, so we need erase that
        std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
        opts.erase(opts.begin());

        /// Parse again
        po::variables_map option_map;
        po::parsed_options stream_subcmd_parsed = po::command_line_parser(opts).options(desc).positional(pos).allow_unregistered().run();
        po::store(stream_subcmd_parsed, option_map);

        std::string stream_subcmd = option_map["subcommand"].as<std::string>();
        if (stream_subcmd == "list")
        {
            return parseStreamListArgs(stream_subcmd_parsed, progname);
        }
        else if (stream_subcmd == "create")
        {
            return parseStreamCreateArgs(stream_subcmd_parsed, progname);
        }
        else if (stream_subcmd == "delete")
        {
            return parseStreamDeleteArgs(stream_subcmd_parsed, progname);
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

        /// native_log produce --namespace <namespace> --stream <stream-name> --num_records <total-records>
        /// --batch_size <batch_size> --concurrency <concurrency>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the stream");
        options("stream", value<std::string>(), "stream name");
        options("num_records", value<int64_t>()->default_value(1000000), "total number of records");
        options("record_batch_size", value<int64_t>()->default_value(100), "Number of records in one batch");
        options(
            "concurrency",
            value<int64_t>()->default_value(1),
            "Number of produce threads. Each thread will have a fair share of the total records");
        options("validate_sns", value<bool>()->default_value(false), "if validate record sequences");

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
        for (const auto & key : {"namespace", "stream"})
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
        args.stream = option_map["stream"].as<std::string>();
        args.num_records = option_map["num_records"].as<int64_t>();
        args.record_batch_size = option_map["record_batch_size"].as<int64_t>();
        args.concurrency = option_map["concurrency"].as<int64_t>();
        args.validate_sns = option_map["validate_sns"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "produce";
        nl_args.produce_args = args;
        return nl_args;
    }

    NativeLogArgs parseConsumeArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("consume options", getTerminalWidth());

        /// native_log consume --namespace <namespace> --stream <stream-name> --num_records <total-records>
        /// --start_sn [-1|-2|sn] --concurrency <concurrency>
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the stream");
        options("stream", value<std::string>(), "stream name");
        options("start_sn", value<int64_t>()->default_value(-2), "start sequence to consume from");
        options("num_records", value<int64_t>()->default_value(1000000), "total number of records");
        options("buf_size", value<int64_t>()->default_value(50*1024*1024), "buffer size to read records");
        options("single_thread", value<bool>()->default_value(false), "Use single thread to consume. Otherwise one thread per partition");
        options("validate_sns", value<bool>()->default_value(false), "if validate record sequences");

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
        for (const auto & key : {"namespace", "stream"})
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
        args.stream = option_map["stream"].as<std::string>();
        args.start_sn = option_map["start_sn"].as<int64_t>();
        args.num_records = option_map["num_records"].as<int64_t>();
        args.buf_size = option_map["buf_size"].as<int64_t>();
        args.single_thread = option_map["single_thread"].as<bool>();
        args.validate_sns = option_map["validate_sns"].as<bool>();

        NativeLogArgs nl_args;
        nl_args.command = "consume";
        nl_args.consume_args = args;
        return nl_args;
    }

    NativeLogArgs parseTrimArgs(po::parsed_options & cmd_parsed, const char * progname)
    {
        using boost::program_options::value;

        po::options_description desc = createOptionsDescription("consume options", getTerminalWidth());

        /// native_log trim --namespace <namespace> --stream <stream-name> --to_sn [sn]
        auto options = desc.add_options();
        options("help", "help message");
        options("namespace", value<std::string>(), "namespace of the stream");
        options("stream", value<std::string>(), "stream name");
        options("to_sn", value<int64_t>()->default_value(0), "Offset to trim to");

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
        for (const auto & key : {"namespace", "stream"})
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
        args.stream = option_map["stream"].as<std::string>();
        args.to_sn = option_map["to_sn"].as<int64_t>();

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
    /// cmds("data_dir", value<std::string>()->default_value("/var/log/proton/nativelog/log"), "Log data directory");
    /// cmds("meta_dir", value<std::string>()->default_value("/var/log/proton/nativelog/meta"), "Log metadata directory");
    cmds("command", value<std::string>()->default_value("help"), "<stream|produce|consume|trim> subcommand to execute");
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
    else if (cmd == "stream")
    {
        return parseStreamArgs(cmd_parsed, argv[0]);
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
