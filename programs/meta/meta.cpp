#include "meta_cli_parser.h"

#include <Cluster/Common/CallResult.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <IO/ReadHelpers.h>
#include <Common/logger_useful.h>

#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

namespace
{

const cluster::StreamShard META_STREAM_SHARD{"__tp_sys", "__tp_mlog", cluster::StreamID{UInt128(1)}, /*shard_=*/0};

class Meta
{
public:
    Meta(cluster::meta::MetaArgs meta_args_, LoggerPtr logger_)
        : meta_args(std::move(meta_args_))
        , meta_db(meta_args.dir, META_STREAM_SHARD.id_shard, /*metadata_keep_version=*/10, logger_)
        , logger(logger_)
    {
    }

    void run()
    {
        switch (meta_args.cmd)
        {
            case cluster::meta::MetaCommand::Examine:
                return handleExamineCommand();
            case cluster::meta::MetaCommand::Delete:
                return handleDeleteCommand();
            default:
                LOG_ERROR(logger, "Unsupported command {}", meta_args.cmd);
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported command {}", meta_args.cmd);
        }
    }

private:
    void handleExamineCommand()
    {
        if (meta_args.key_space)
        {
            examineOneKeySpace(meta_args.key_space.value(), meta_args.key_space_name);
        }
        else
        {
            for (const auto & [keyspace_name, keyspace] : cluster::meta::keySpaces())
                examineOneKeySpace(keyspace, keyspace_name);
        }
    }

    void examineOneKeySpace(cluster::MetaKeySpace keyspace, const std::string & keyspace_name)
    {
        std::vector<std::string> corrupted_keys;

        auto last_applied_sn_result = meta_db.loadAppliedSequence();
        if (last_applied_sn_result.hasError())
        {
            LOG_ERROR(
                logger,
                "Failed to examine meta object keyspace={} because loading last applied sn failed, {}",
                keyspace_name,
                last_applied_sn_result.errorString());
            return;
        }

        LOG_INFO(
            logger, "Examine meta objects in keyspace={} last_applied_sn={{{}}}", keyspace_name, last_applied_sn_result.result.string());

        switch (keyspace)
        {
            case cluster::MetaKeySpace::Stream:
            {
                auto result = meta_db.listStreams(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine streams, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} streams, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::Database:
            {
                auto result = meta_db.listDatabases(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine databases, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} databases, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::UDF:
            {
                auto result = meta_db.listUserDefinedFunctions(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine user defined functions, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} user defined functions, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::AccessEntity:
            {
                auto result = meta_db.listAccessEntities(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine access entities, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} access entities, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::FormatSchema:
            {
                auto result = meta_db.listFormatSchemas(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine format schemas, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} format schemas, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::Disk:
            {
                auto result = meta_db.listDisks(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine disks, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} disks, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::StoragePolicy:
            {
                auto result = meta_db.listStoragePolicies(&corrupted_keys);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine storage policies, {}", result.errorString());

                LOG_INFO(
                    logger,
                    "Examined {} storage policies, {} corrupted, corrupted_keys=[{}]",
                    result.result.size(),
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));

                if (meta_args.dump)
                    dump(result.result);

                break;
            }
            case cluster::MetaKeySpace::Node:
            {
                LOG_INFO(logger, "Node listing skipped - no nodes to list");
                LOG_INFO(
                    logger,
                    "Examined {} nodes, {} corrupted, corrupted_keys=[{}]",
                    0,
                    corrupted_keys.size(),
                    fmt::join(corrupted_keys, "\n"));


                break;
            }
            default:
            {
                LOG_ERROR(logger, "Examining keyspace={} is not supported", keyspace_name);
            }
        }
    }

    void handleDeleteCommand()
    {
        auto last_applied_sn_result = meta_db.loadAppliedSequence();
        if (last_applied_sn_result.hasError() || last_applied_sn_result.result.sn == 0)
        {
            LOG_ERROR(
                logger,
                "Failed to delete meta object keyspace={} ns={} key={} key2={}, because loading last applied sn failed, {}",
                meta_args.key_space_name,
                meta_args.ns,
                meta_args.key,
                meta_args.key2,
                last_applied_sn_result.errorString());
            return;
        }

        LOG_INFO(
            logger,
            "Delete meta object keyspace={} ns={} key={} key2={} last_applied_sn={{{}}}",
            meta_args.key_space_name,
            meta_args.ns,
            meta_args.key,
            meta_args.key2,
            last_applied_sn_result.result.string());

        std::cout << "Press Y to delete: ";
        auto ch = std::getchar();
        if (ch != 'Y')
            return;

        switch (*meta_args.key_space)
        {
            case cluster::MetaKeySpace::Stream:
            {
                auto result = meta_db.deleteStream(meta_args.ns, meta_args.key, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to delete streams, {}", result.string());
                else
                    LOG_ERROR(logger, "Successfully delete stream");

                break;
            }
            case cluster::MetaKeySpace::UDF:
            {
                auto result = meta_db.deleteUserDefinedFunction(meta_args.key, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to delete user defined function, {}", result.string());
                else
                    LOG_ERROR(logger, "Successfully delete user defined function");

                break;
            }
            case cluster::MetaKeySpace::AccessEntity:
            {
                DB::UUID uuid;
                if (!DB::tryParse(uuid, meta_args.key))
                {
                    LOG_ERROR(logger, "Invalid uuid {}", meta_args.key);
                    return;
                }

                auto result = meta_db.deleteAccessEntity(uuid, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to delete access entity, {}", result.string());
                else
                    LOG_INFO(logger, "Successfully delete access entity");

                break;
            }
            case cluster::MetaKeySpace::FormatSchema:
            {
                if (meta_args.key2.empty())
                {
                    LOG_ERROR(logger, "format name is required to delete a format schema");
                    return;
                }
                auto result = meta_db.deleteFormatSchema(meta_args.key, meta_args.key2, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to delete format schemas, {}", result.string());
                else
                    LOG_INFO(logger, "Successfully delete format schemas");

                break;
            }
            case cluster::MetaKeySpace::Disk:
            {
                auto result = meta_db.deleteDisk(meta_args.key, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to delete disk, {}", result.string());
                else
                    LOG_INFO(logger, "Successfully to delete disk");

                break;
            }
            case cluster::MetaKeySpace::StoragePolicy:
            {
                auto result = meta_db.deleteStoragePolicy(meta_args.key, last_applied_sn_result.result);
                if (result.hasError())
                    LOG_ERROR(logger, "Failed to examine storage policy, {}", result.string());
                else
                    LOG_INFO(logger, "Successfully delete storage policy");

                break;
            }
            default:
            {
                LOG_ERROR(logger, "Deleting meta object in keyspace={} is not supported", meta_args.key_space_name);
            }
        }
    }

private:
    template <typename V>
    void dump(const V & v)
    {
        LOG_INFO(logger, "------------");
        for (const auto & e : v)
            LOG_INFO(logger, "{}", e->string());
        LOG_INFO(logger, "------------");
    }

private:
    cluster::meta::MetaArgs meta_args;

    cluster::meta::MetaDB meta_db;

    LoggerPtr logger;
};

void run(const cluster::meta::MetaArgs & meta_args, LoggerPtr logger)
{
    Meta meta(meta_args, logger);
    meta.run();
}

}

int mainMeta(int argc, char ** argv)
{
    auto meta_args = cluster::meta::parseArgs(argc, argv);
    if (!meta_args.valid())
        return 1;

    /// Setup logger
    Poco::AutoPtr<OwnPatternFormatter> pf(new OwnPatternFormatter(true));
    Poco::AutoPtr<DB::OwnFormattingChannel> console_channel(new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel));
    Poco::Logger::root().setChannel(console_channel);
    Poco::Logger::root().setLevel("debug");

    auto logger = getLogger("meta");

    try
    {
        run(meta_args, logger);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to run: {} ", e.message());
    }
    catch (const std::exception & e)
    {
        DB::tryLogCurrentException(logger, DB::getExceptionStackTraceString(e));
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, "Unknown exception");
    }
    return 0;
}
