#include <Server/RestRouterHandlers/LogLevelHandler.h>
#include <Server/RestRouterHandlers/SchemaValidator.h>

#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/executeSelectQuery.h>
#include <Loggers/Loggers.h>
#include <Common/LogLevels.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/replace.hpp>
#include <Common/re2.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
extern const int BAD_ARGUMENTS;
extern const int ACCESS_DENIED;
extern const int RESOURCE_NOT_INITED;
extern const int TIMEOUT_EXCEEDED;
}

namespace
{
std::map<String, std::map<String, String>> SET_LOG_LEVEL_SCHEMA
    = {{"required", {{"level", "string"}}}, {"optional", {{"logger_name", "string"}}}};

String buildResponse(const Poco::JSON::Object::Ptr & object, const String & query_id)
{
    Poco::JSON::Object obj;
    obj.set("request_id", query_id);
    if (object.get() != nullptr)
        obj.set("data", object);

    std::stringstream resp_str_stream;
    obj.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}

bool LogLevelHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(SET_LOG_LEVEL_SCHEMA, payload, error_msg))
        return false;

    /// Validate log level
    String lower_level = payload->get("level").toString();
    std::transform(lower_level.begin(), lower_level.end(), lower_level.begin(), [](unsigned char c) { return std::tolower(c); });

    if (!getValidLogLevels().contains(lower_level))
    {
        error_msg = fmt::format("Invalid log level. Valid levels are: {}", fmt::join(getValidLogLevels(), ", "));
        return false;
    }

    return true;
}

std::pair<String, Int32> LogLevelHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    try
    {
        Poco::JSON::Object::Ptr resp = new Poco::JSON::Object();
        Poco::JSON::Array::Ptr loggers_array = new Poco::JSON::Array();

        std::vector<std::string> names;
        names.reserve(512);
        Poco::Logger::root().names(names);

        Poco::JSON::Object::Ptr root_logger = new Poco::JSON::Object();
        root_logger->set("name", "root");
        root_logger->set("level_name", String(InternalTextLogsQueue::getPriorityName(Poco::Logger::root().getLevel())));
        loggers_array->add(root_logger);

        for (const auto & name : names)
        {
            if (!name.empty())
            {
                try
                {
                    auto logger = getLogger(name);
                    Poco::JSON::Object::Ptr logger_obj = new Poco::JSON::Object();
                    logger_obj->set("name", name);
                    logger_obj->set("level_name", String(InternalTextLogsQueue::getPriorityName(logger->getLevel())));
                    loggers_array->add(logger_obj);
                }
                catch (...)
                {
                    continue;
                }
            }
        }

        resp->set("loggers", loggers_array);
        resp->set("total_count", loggers_array->size());

        return {buildResponse(resp, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (const std::exception & e)
    {
        return {
            jsonErrorResponse(fmt::format("Failed to get log levels: {}", e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}

std::pair<String, Int32> LogLevelHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        if (!payload->has("level"))
        {
            return {jsonErrorResponse("Missing required field 'level'", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};
        }

        String level = payload->get("level").toString();
        String logger_name;
        if (payload->has("logger_name"))
            logger_name = payload->get("logger_name").toString();

        if (level.empty())
            return {jsonErrorResponse("Log level cannot be empty", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        if (!logger_name.empty())
        {
            static const re2::RE2 invalid_chars_regex(R"([';\"\\])");
            if (RE2::PartialMatch(logger_name, invalid_chars_regex))
            {
                return {
                    jsonErrorResponse("Logger name contains invalid characters", ErrorCodes::BAD_REQUEST_PARAMETER),
                    HTTPResponse::HTTP_BAD_REQUEST};
            }

            if (!Poco::Logger::has(logger_name))
            {
                /// If exact match fails, try pattern matching as fallback
                auto matched_loggers = Loggers::findLoggersByPattern(logger_name);

                if (matched_loggers.empty())
                {
                    return {
                        jsonErrorResponse(
                            fmt::format(
                                "No loggers found matching pattern '{}'. Use SYSTEM SHOW LOGGERS to check existing loggers first.",
                                logger_name),
                            ErrorCodes::BAD_ARGUMENTS),
                        HTTPResponse::HTTP_BAD_REQUEST};
                }
            }
        }

        String query;
        if (logger_name.empty())
        {
            query = fmt::format("SYSTEM SET LOG LEVEL '{}'", level);
        }
        else
        {
            /// Escape logger name with single quotes and escape any existing single quotes
            String escaped_logger_name = logger_name;
            boost::replace_all(escaped_logger_name, "'", "''");
            query = fmt::format("SYSTEM SET LOG LEVEL '{}' FOR '{}'", level, escaped_logger_name);
        }

        LOG_DEBUG(log, "Executing log level change query: {}", query);

        try
        {
            executeNonInsertQuery(query, query_context, /*callback=*/{}, /*internal=*/true);
        }
        catch (const Exception & e)
        {
            Int32 http_status;
            auto error_code = e.code();

            if (error_code == ErrorCodes::BAD_ARGUMENTS)
                http_status = HTTPResponse::HTTP_BAD_REQUEST;
            else if (error_code == ErrorCodes::ACCESS_DENIED)
                http_status = HTTPResponse::HTTP_FORBIDDEN;
            else if (error_code == ErrorCodes::RESOURCE_NOT_INITED)
                http_status = HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            else if (error_code == ErrorCodes::TIMEOUT_EXCEEDED)
                http_status = HTTPResponse::HTTP_REQUEST_TIMEOUT;
            else
                http_status = HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;

            return {jsonErrorResponse(fmt::format("Failed to set log level: {}", e.message()), e.code()), http_status};
        }

        /// Success response with detailed information
        Poco::JSON::Object::Ptr resp = new Poco::JSON::Object();
        resp->set("success", true);
        resp->set(
            "message",
            fmt::format(
                "Log level changed to '{}' for {}", level, logger_name.empty() ? "all loggers" : fmt::format("logger '{}'", logger_name)));
        resp->set("level", level);
        resp->set("logger_name", logger_name.empty() ? "all" : logger_name);
        resp->set("query", query);

        return {buildResponse(resp, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (const std::exception & e)
    {
        return {
            jsonErrorResponse(fmt::format("Unexpected error: {}", e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
    catch (...)
    {
        return {jsonErrorResponse("Unknown error occurred", ErrorCodes::UNKNOWN_EXCEPTION), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}
}
