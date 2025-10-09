#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ChangeLogLevelRequest.h>
#include <Common/LogLevels.h>
#include <Common/Stopwatch.h>
#include <Daemon/BaseDaemon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int BAD_ARGUMENTS;
extern const int RESOURCE_NOT_INITED;
extern const int UNKNOWN_EXCEPTION;
}

void MetadataUpdater::handleChangeLogLevel(
    const cluster::ChangeLogLevelRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & data = request.data();

    LOG_INFO(logger, "Start ChangeLogLevel, {}", data.doString());
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End ChangeLogLevel, {} took={}ms", data.doString(), stopwatch.elapsedMilliseconds()); });

    try
    {
        if (data.log_level.empty())
        {
            std::string error_msg = "Log level cannot be empty";
            LOG_ERROR(logger, "Failed to change log level {}: {}", data.doString(), error_msg);
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::BAD_ARGUMENTS, std::move(error_msg));
            return;
        }

        std::string lower_level = data.log_level;
        std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(), [](unsigned char c) { return std::tolower(c); });

        if (!getValidLogLevels().contains(lower_level))
        {
            std::string error_msg
                = fmt::format("Invalid log level '{}'. Valid levels are: {}", data.log_level, fmt::join(getValidLogLevels(), ", "));
            LOG_ERROR(logger, "Failed to change log level {}: {}", data.doString(), error_msg);
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::BAD_ARGUMENTS, std::move(error_msg));
            return;
        }

        auto daemon_instance = BaseDaemon::tryGetInstance();
        if (!daemon_instance)
        {
            std::string error_msg = "BaseDaemon instance not available - system may be shutting down";
            LOG_ERROR(logger, "Failed to change log level {}: {}", data.doString(), error_msg);
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, DB::ErrorCodes::RESOURCE_NOT_INITED, std::move(error_msg));
            return;
        }

        daemon_instance->get().setLogLevel(lower_level, data.logger_name);

        LOG_INFO(
            logger,
            "Successfully changed log level to '{}' for logger '{}'",
            lower_level,
            data.logger_name.empty() ? "all loggers" : data.logger_name);
    }
    catch (const DB::Exception & e)
    {
        std::string error_msg = fmt::format("Database exception while changing log level: {}", e.message());
        LOG_ERROR(logger, "Failed to change log level {}: {}", data.doString(), error_msg);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::move(error_msg));
        return;
    }
    catch (...)
    {
        std::string error_msg = "Unknown exception occurred while changing log level";
        tryLogCurrentException(logger, fmt::format("Failed to change log level {}", data.doString()));
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_EXCEPTION, std::move(error_msg));
        return;
    }

    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

}
