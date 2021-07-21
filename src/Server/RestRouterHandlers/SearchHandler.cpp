#include "SearchHandler.h"

#include "SchemaValidator.h"

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/executeQuery.h>

#include <re2/re2.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
}

namespace
{
    const std::map<String, std::map<String, String>> SEARCH_SCHEMA
        = {{"required", {{"query", "string"}}},
           {"optional", {{"mode", "string"}, {"end_time", "string"}, {"start_time", "string"}, {"offset", "int"}, {"page_size", "int"}}}};
}

void SearchHandler::execute(const Poco::JSON::Object::Ptr & payload, HTTPServerResponse & response) const
{
    auto send_error = [&response](const String && error_msg) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        response.setStatusAndReason(HTTPResponse::HTTP_BAD_REQUEST);
        *response.send() << error_msg << std::endl;
    };

    String error;
    if (!validatePost(payload, error))
        return send_error(jsonErrorResponse(error, ErrorCodes::BAD_REQUEST_PARAMETER));

    /// FIXME : enforce SELECT query at low level to avoid this sql parsing here
    const auto & query = getQuery(payload);
    LOG_INFO(log, "Execute query: {}", query);
    ReadBufferFromString in{query};

    /// Prepare output buffer
    const auto out = getOutputBuffer(response);
    try
    {
        /// FIXME: implement outbound solution to support query progress
        executeQuery(in, *out, /* allow_into_outfile = */ false, query_context, {});
    }
    catch (Exception & e)
    {
        tryLogCurrentException(log);
        /// Send the error message into already used (and possibly compressed) stream.
        /// Note that the error message will possibly be sent after some data.
        /// Also HTTP code 200 could have already been sent.
        Int32 http_status = HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
        response.setStatusAndReason(HTTPResponse::HTTPStatus(http_status));
        bool data_sent = out->count() != out->offset();

        /// If buffer has data, and that data wasn't sent yet, then no need to send that data
        if (!data_sent)
            out->position() = out->buffer().begin();

        writeString(jsonErrorResponse(e.message(), e.code()), *out);
        writeChar('\n', *out);
        out->next();
        out->finalize();
    }
}

String SearchHandler::getQuery(const Poco::JSON::Object::Ptr & payload) const
{
    /// Setup query settings
    const auto & mode = payload->has("mode") ? payload->get("mode").toString() : "verbose";

    /// FIXME: to support 'realtime' mode in future
    if (mode == "verbose")
    {
        query_context->setSetting("asterisk_include_materialized_columns", true);
        query_context->setSetting("asterisk_include_alias_columns", true);
    }

    const auto & start_time = payload->has("start_time") ? payload->get("start_time").toString() : "";
    if (!start_time.empty())
        query_context->setTimeParamStart(start_time);

    const auto & end_time = payload->has("end_time") ? payload->get("end_time").toString() : "";
    if (!end_time.empty())
        query_context->setTimeParamEnd(end_time);

    query_context->setSetting("unnest_subqueries", true);

    /// Setup settings passed by query params
    setQuerySettings();

    std::vector<String> query_parts;
    query_parts.push_back(fmt::format("SELECT * FROM ({})", payload->get("query").toString()));
    if (payload->has("offset") && payload->has("page_size"))
    {
        int offset = payload->getValue<int>("offset");
        int page_size = payload->getValue<int>("page_size");
        if (page_size > 0 && offset >= 0)
            query_parts.push_back(fmt::format("LIMIT {}, {}", offset, page_size));
    }
    return boost::algorithm::join(query_parts, " ");
}

bool SearchHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(SEARCH_SCHEMA, payload, error_msg))
        return false;

    if (payload->has("mode"))
    {
        const auto & mode = payload->getValue<String>("mode");
        if (mode != "standard" && mode != "verbose")
        {
            error_msg = fmt::format("Invalid 'mode': {}, only support 'standard', 'verbose'", mode);
            return false;
        }
    }

    bool has_limit = payload->has("offset");
    bool has_page_size = payload->has("page_size");
    if (has_limit && has_page_size)
    {
        if (payload->getValue<int>("offset") < 0 || payload->getValue<int>("page_size") <= 0)
        {
            error_msg = "Invalid 'limit' or 'page_size";
            return false;
        }
    }
    else if (has_limit || has_page_size)
    {
        error_msg = "Missing 'limit' or 'page_size";
        return false;
    }

    return true;
}

std::shared_ptr<WriteBufferFromHTTPServerResponse> SearchHandler::getOutputBuffer(HTTPServerResponse & response) const
{
    const auto & settings = query_context->getSettingsRef();
    const auto & accept_encodings = getAcceptEncoding();
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!accept_encodings.empty())
    {
        /// If client supports brotli - it's preferred.
        /// Both gzip and deflate are supported. If the client supports both, gzip is preferred.
        /// NOTE parsing of the list of methods is slightly incorrect.
        if (std::string::npos != accept_encodings.find("br"))
            http_response_compression_method = CompressionMethod::Brotli;
        else if (std::string::npos != accept_encodings.find("gzip"))
            http_response_compression_method = CompressionMethod::Gzip;
        else if (std::string::npos != accept_encodings.find("deflate"))
            http_response_compression_method = CompressionMethod::Zlib;
        else if (std::string::npos != accept_encodings.find("xz"))
            http_response_compression_method = CompressionMethod::Xz;
        else if (std::string::npos != accept_encodings.find("zstd"))
            http_response_compression_method = CompressionMethod::Zstd;
    }

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;
    unsigned keep_alive_timeout = 10;

    auto out = std::make_shared<WriteBufferFromHTTPServerResponse>(
        response, false, keep_alive_timeout, client_supports_http_compression, http_response_compression_method);
    out->setSendProgressMode(SendProgressMode::progress_none);

    if (client_supports_http_compression)
        out->setCompressionLevel(settings.http_zlib_compression_level);

    return out;
}

void SearchHandler::setQuerySettings() const
{
    static const NameSet reserved_param_names{
        "compress",
        "decompress",
        "user",
        "password",
        "quota_key",
        "query_id",
        "stacktrace",
        "buffer_size",
        "wait_end_of_query",
        "session_id",
        "session_timeout",
        "session_check"};

    auto param_could_be_skipped = [&](const String & name) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        if (reserved_param_names.count(name))
            return true;

        return false;
    };

    const auto & query_id = getQueryParameter("x-daisy-request-id");
    if (!query_id.empty())
        query_context->setCurrentQueryId(query_id);

    /// FIXME to support cascaded write buffer and session
    SettingsChanges settings_changes;
    String default_format = "JSONCompact";
    for (const auto & [key, value] : *query_parameters)
    {
        if (param_could_be_skipped(key))
            continue;

        if (key == "default_format" && !value.empty())
            default_format = value;
        else
        {
            /// Other than query parameters are treated as settings.
            if (startsWith(key, "param_"))
            {
                /// Save name and values of substitution in dictionary.
                const String parameter_name = key.substr(strlen("param_"));
                settings_changes.push_back({parameter_name, value});
            }
            else
                settings_changes.push_back({key, value});
        }
    }

    query_context->setDefaultFormat(default_format);
    query_context->checkSettingsConstraints(settings_changes);
    query_context->applySettingsChanges(settings_changes);
}
}
