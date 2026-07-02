#include <Server/RestRouterHandlers/SearchHandler.h>

#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/RestRouterHandlers/SchemaValidator.h>

#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/executeQuery.h>

#include <Common/re2.h>
#include <fmt/ranges.h>


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
        *response.send() << error_msg << "\n";
    };

    String error;
    if (!validatePost(payload, error))
        return send_error(jsonErrorResponse(error, ErrorCodes::BAD_REQUEST_PARAMETER));

    /// FIXME : enforce SELECT query at low level to avoid this sql parsing here
    const auto & query = getQuery(payload);
    LOG_DEBUG(log, "Execute query: {}", query);
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
    }
    out->finalize();
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
    else if (mode == "table")
        query_context->setSetting("query_mode", mode);

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
    return fmt::format("{}", fmt::join(query_parts, " "));
}

bool SearchHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(SEARCH_SCHEMA, payload, error_msg))
        return false;

    if (payload->has("mode"))
    {
        const auto & mode = payload->getValue<String>("mode");
        if (mode != "standard" && mode != "verbose" && mode != "table")
        {
            error_msg = fmt::format("Invalid 'mode': {}, only support 'standard', 'verbose', 'table'", mode);
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

std::unique_ptr<WriteBuffer> SearchHandler::getOutputBuffer(HTTPServerResponse & response) const
{
    std::unique_ptr<WriteBuffer> out = std::make_unique<WriteBufferFromHTTPServerResponse>(response, /*is_http_method_head_=*/false);

    const auto & settings = query_context->getSettingsRef();
    const auto & accept_encodings = getAcceptEncoding();
    const CompressionMethod http_response_compression_method = chooseHTTPCompressionMethod(accept_encodings);
    const bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;
    if (client_supports_http_compression)
    {
        out = wrapWriteBufferWithCompressionMethod(
            std::move(out),
            http_response_compression_method,
            static_cast<int>(settings.http_zlib_compression_level),
            static_cast<int>(settings.output_format_compression_zstd_window_log));
    }

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

        if (reserved_param_names.contains(name))
            return true;

        return false;
    };

    const auto & query_id = getQueryParameter("x-timeplus-request-id", getQueryParameter("x-proton-request-id"));
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
