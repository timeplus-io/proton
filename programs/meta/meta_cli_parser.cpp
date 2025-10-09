#include "meta_cli_parser.h"

#include <Common/Exception.h>
#include <Common/TerminalSize.h>

#include <boost/program_options.hpp>

#include <iostream>
#include <ranges>


namespace cluster::meta
{

namespace
{

const std::unordered_map<std::string, cluster::MetaKeySpace> KEYSPACE_MAP = {
    {"Stream", cluster::MetaKeySpace::Stream},
    {"UDF", cluster::MetaKeySpace::UDF},
    {"AccessEntity", cluster::MetaKeySpace::AccessEntity},
    {"FormatSchema", cluster::MetaKeySpace::FormatSchema},
    {"Database", cluster::MetaKeySpace::Database},
    {"Disk", cluster::MetaKeySpace::Disk},
    {"StoragePolicy", cluster::MetaKeySpace::StoragePolicy},
    {"Node", cluster::MetaKeySpace::Node},
    {"MetaAppliedSN", cluster::MetaKeySpace::MetaAppliedSN},
    {"MaterializedViewAssignment", cluster::MetaKeySpace::MaterializedViewAssignment},
};

std::string keyspaceUsage()
{
    auto keyspaces = KEYSPACE_MAP | std::views::transform([](const auto & kv) { return kv.first; });
    return fmt::format("keyspace of the metadata, supported namespaces=[{}]", fmt::join(keyspaces, ", "));
}

void printUsage(const char * prog, const po::options_description & desc)
{
    std::cerr << "Usage: " << prog << " " << desc << std::endl;
}

MetaArgs parseExamineArgs(po::parsed_options & cmd_parsed, const char * progname)
{
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("examine options", getTerminalWidth());

    /// meta examine --keyspace <key-space>
    auto options = desc.add_options();
    options("help", "help message");

    auto keyspaces = keyspaceUsage();
    options("keyspace", value<std::string>()->default_value(""), keyspaces.c_str());
    options("dump", value<bool>()->default_value(false), "if dump the meta object to stdout");

    std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
    opts.erase(opts.begin());

    /// Parse examine args
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

    MetaArgs args;

    args.cmd = MetaCommand::Examine;

    auto key_space = option_map["keyspace"].as<std::string>();
    if (!key_space.empty())
    {
        auto iter = KEYSPACE_MAP.find(key_space);
        if (iter == KEYSPACE_MAP.end())
        {
            printUsage(progname, desc);
            return {};
        }
        else
        {
            args.key_space = iter->second;
            args.key_space_name.swap(key_space);
        }
    }

    args.dump = option_map["dump"].as<bool>();

    return args;
}

MetaArgs parseDeleteArgs(po::parsed_options & cmd_parsed, const char * progname)
{
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("delete options", getTerminalWidth());

    /// meta delete --keyspace <key-space> --namespace <namespace> --key <name> --key2 <associated_key>
    auto options = desc.add_options();
    options("help", "help message");

    auto keyspaces = keyspaceUsage();
    options("keyspace", value<std::string>(), keyspaces.c_str());
    options("namespace", value<std::string>()->default_value(""), "namespace of the metadata");
    options("key", value<std::string>(), "metadata key if examine one metadata object");
    options(
        "key2",
        value<std::string>()->default_value(""),
        "second metadata key if examine one metadata object and if it has a second combo key");

    std::vector<std::string> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
    opts.erase(opts.begin());

    /// Parse examine args
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

    MetaArgs args;

    args.cmd = MetaCommand::Delete;

    auto key_space = option_map["keyspace"].as<std::string>();
    if (!key_space.empty())
    {
        auto iter = KEYSPACE_MAP.find(key_space);
        if (iter == KEYSPACE_MAP.end())
        {
            printUsage(progname, desc);
            return {};
        }
        else
        {
            args.key_space = iter->second;
            args.key_space_name.swap(key_space);
        }
    }

    if (!args.key_space)
    {
        printUsage(progname, desc);
        return {};
    }

    for (const auto * required : {"namespace", "key"})
    {
        if (!option_map.contains(required))
        {
            printUsage(progname, desc);
            return {};
        }
    }

    args.ns = option_map["namespace"].as<std::string>();
    args.key = option_map["key"].as<std::string>();
    args.key2 = option_map["key2"].as<std::string>();

    if (args.ns.empty() || args.key.empty())
    {
        printUsage(progname, desc);
        return {};
    }

    return args;
}

}


MetaArgs parseArgs(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description global = createOptionsDescription("Global options", getTerminalWidth());
    auto cmds = global.add_options();
    cmds("dir", value<std::string>()->default_value(""), "MetaStore data directory");
    cmds("command", value<std::string>()->default_value("help"), "<examine|delete> subcommand to execute");
    cmds("subargs", value<std::vector<std::string>>(), "Arguments for subcommand");

    po::positional_options_description pos;
    pos.add("command", 1).add("subargs", -1);

    po::variables_map option_map;
    po::parsed_options cmd_parsed = po::command_line_parser(argc, argv).options(global).positional(pos).allow_unregistered().run();
    po::store(cmd_parsed, option_map);

    std::string metastore_dir = option_map["dir"].as<std::string>();
    if (metastore_dir.empty())
    {
        printUsage(argv[0], global);
        return {};
    }

    std::string cmd = option_map["command"].as<std::string>();
    if (cmd == "examine")
    {
        auto meta_args = parseExamineArgs(cmd_parsed, argv[0]);
        meta_args.dir.swap(metastore_dir);
        return meta_args;
    }
    else if (cmd == "delete")
    {
        auto meta_args = parseDeleteArgs(cmd_parsed, argv[0]);
        meta_args.dir.swap(metastore_dir);
        return meta_args;
    }
    else
    {
        printUsage(argv[0], global);
        return {};
    }
}

const std::unordered_map<std::string, cluster::MetaKeySpace> & keySpaces()
{
    return KEYSPACE_MAP;
}

}
