#include <Storages/ExternalStream/HTTP/HTTPSink.h>

#include <IO/ReadBufferFromIStream.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromHTTP.h>
#include <IO/WriteBufferWithNextCallback.h>
#include <Processors/Executors/MessageQueueFormatExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ExternalStream/HTTP/HTTPConfiguration.h>
#include <Common/Base64.h>

#include <base/JSON.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OPENSEARCH_ERROR;
}

namespace ExternalStream
{

namespace
{

/// https://opensearch.org/docs/latest/api-reference/document-apis/bulk/#example-response
void checkOpenSearchResponse(std::istream * resp_body)
{
    std::list<String> errors;

    ReadBufferFromIStream buf{*resp_body};
    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    JSON body{json_str};
    if (body.empty())
        return;

    if (!body.has("errors"))
        return;

    if (!body["errors"].getBool())
        return;

    for (const auto item : body["items"])
    {
        auto create_result = item["create"];
        if (create_result.empty())
            continue;

        if (create_result["status"].getInt() >= 300)
            errors.push_back(create_result.toString());
    }

    if (!errors.empty())
        throw Exception(ErrorCodes::OPENSEARCH_ERROR, "Bulk ingestion returned errors: {}", fmt::join(errors, ","));
}

}

HTTPSink::HTTPSink(
    const HTTPConfiguration & http_config_,
    const Block & sample_block,
    const BatchConfiguration & batch_config_,
    const ContextPtr & context_)
    : SinkToStorage(sample_block, ProcessorID::HTTPSink)
    , http_config(http_config_)
    , batch_config(batch_config_)
    , uri(http_config.uri)
    , session(makePooledHTTPSession(
          uri,
          http_config.client_key_file,
          http_config.ca_cert_file,
          /*ca_location=*/"",
          http_config.insecure_connection ? Poco::Net::Context::VerificationMode::VERIFY_NONE
                                          : Poco::Net::Context::VerificationMode::VERIFY_RELAXED,
          http_config.timeouts,
          /*per_endpoint_pool_size=*/10))
    , context(context_)
{
    session->setKeepAlive(true);
    session->setKeepAliveTimeout(http_config.timeouts.http_keep_alive_timeout);

    if (auto it = std::ranges::find_if(http_config.headers, [](const auto & header) { return header.name == "Content-Type"; });
        it != http_config.headers.end())
        content_type = it->value;
    else
        content_type = FormatFactory::instance().getContentType(http_config.format, getHeader(), context, http_config.format_settings);

    content_encoding = toContentEncodingName(http_config.compression_method);

    if (const auto & user_info = uri.getUserInfo(); !user_info.empty())
        http_config.headers.emplace_back("Authorization", fmt::format("Basic {}", DB::base64Encode(user_info)));

    format_executor = std::make_unique<MessageQueueFormatExecutor>(
        getHeader(), http_config.format, http_config.format_settings, batch_config.count_threshold, batch_config.bytes_threshold, context);
    format_executor->start();
}

HTTPSink::~HTTPSink()
{
    try
    {
        if (!isCancelled())
            onFinish();
        format_executor->finish();
    }
    catch (...)
    {
        tryLogCurrentException("httpsink", "Failed in ~HTTPSink");
    }
}

void HTTPSink::consume(Chunk chunk)
{
    if (isCancelled())
        return;

    if (request_timer.elapsedMilliseconds() >= batch_config.timeout_ms)
        flush();

    auto rows = chunk.rows();
    if (rows == 0)
        return;

    format_executor->execute(
        getHeader().cloneWithColumns(chunk.detachColumns()), [this](const String & message, size_t /*rows_in_message*/) { send(message); });
}

void HTTPSink::send(const String & message) const
{
    auto headers = http_config.headers;
    if (!http_config.use_chunked_transfer_encoding)
        headers.emplace_back(Poco::Net::HTTPMessage::CONTENT_LENGTH, toString(message.size()));

    auto http_buffer
        = std::make_unique<WriteBufferFromHTTP>(session, uri, http_config.http_method, content_type, content_encoding, headers);

    if (http_config.format == "OpenSearch")
        http_buffer->withResponseBodyHandler(checkOpenSearchResponse);

    const auto & settings = context->getSettingsRef();

    auto write_buffer = wrapWriteBufferWithCompressionMethod(
        std::move(http_buffer),
        http_config.compression_method,
        static_cast<int>(settings.output_format_compression_level),
        static_cast<int>(settings.output_format_compression_zstd_window_log));

    write_buffer->write(message.data(), message.size());
    write_buffer->finalize();
}

void HTTPSink::checkpoint(CheckpointContextPtr checkpoint_context)
{
    flush();
    IProcessor::checkpoint(checkpoint_context);
}

void HTTPSink::flush()
{
    SCOPE_EXIT(request_timer.restart());

    onFinish();
}

void HTTPSink::onFinish()
{
    format_executor->flush([this](const String & message, size_t /*rows_in_message*/) { send(message); });
}

}

}
