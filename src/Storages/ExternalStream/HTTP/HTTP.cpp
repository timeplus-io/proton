#include <Storages/ExternalStream/HTTP/HTTP.h>

#include <IO/HTTPCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/ExternalStream/HTTP/HTTPSink.h>
#include <Common/Base64.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int INVALID_SETTING_VALUE;
}

namespace ExternalStream
{

namespace
{

ConnectionTimeouts getHTTPTimeouts(ContextPtr context)
{
    auto http_keepalive_timeout
        = !context->getSettingsRef().http_keep_alive_timeout.changed && context->getConfigRef().has("keep_alive_timeout")
        ? context->getConfigRef().getUInt("keep_alive_timeout")
        : context->getSettingsRef().http_keep_alive_timeout.value.seconds();

    return ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), {http_keepalive_timeout, 0});
}

}

HTTP::HTTP(
    StorageID storage_id,
    StorageInMemoryMetadata metadata,
    std::unique_ptr<ExternalStreamSettings> settings_,
    ASTStorage * storage_def,
    bool attach,
    ContextPtr context)
    : StorageExternalStreamImpl(std::move(storage_id), std::move(metadata), std::move(settings_), context)
    , timeouts(getHTTPTimeouts(context))
{
    batch_config.loadFromContext(context);

    auto method = settings->write_method.value;
    if (method.empty())
        write_method = Poco::Net::HTTPRequest::HTTP_POST;
    else
        write_method = Poco::toUpper(method);

    auto url = settings->write_url.value;
    if (url.empty())
        url = settings->url.value;
    write_url = std::move(url);

    /// Path should never be empty.
    if (auto uri = Poco::URI(write_url); uri.getPath().empty())
        write_url += "/";

    compression_method = chooseCompressionMethod(/*path=*/"", settings->compression_method);

    if (auto ca_pem = settings->ssl_ca_pem.value; !ca_pem.empty())
    {
        ca_file = tmpdir / "ca.pem";
        WriteBufferFromFile(ca_file).write(ca_pem.data(), ca_pem.size());
    }
    else if (!settings->ssl_ca_cert_file.value.empty())
    {
        ca_file = settings->ssl_ca_cert_file.value;
    }

    if (auto key_pem = settings->client_key.value; !key_pem.empty())
    {
        key_file = tmpdir / "key.pem";
        WriteBufferFromFile(key_file).write(key_pem.data(), key_pem.size());
    }

    skip_ssl_verification = settings->skip_ssl_cert_check;

    for (const auto & setting : storage_def->settings->changes)
    {
        if (setting.name.starts_with("http_header_"))
        {
            if (setting.value.getType() != Field::Types::Which::String)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "The value of {} must be a string, but got {}",
                    setting.name,
                    setting.value.getTypeName());

            auto name = setting.name.substr(12); /// remove the prefix
            std::ranges::replace(name, '_', '-'); /// HTTP headers use `-` not `_`
            headers.push_back({name, setting.value.get<String>()});
        }
    }

    if (!settings->username.value.empty())
    {
        if (auto it = std::ranges::find_if(headers, [](const auto & header) { return header.name == "Authorization"; });
            it != headers.end())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Settings conflict: username and http_header_Authorization are both set");

        auto credentials = DB::base64Encode(fmt::format("{}:{}", settings->username.value, settings->password.value));
        headers.push_back({"Authorization", fmt::format("Basic {}", std::move(credentials))});
    }

    /// Validate only on creation
    if (!attach)
    {
        if (write_url.empty())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Settings `url` and `wrtie_url` cannot be both empty");
    }
}

Pipe HTTP::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Reading from HTTP external stream is not implemented yet");
}

SinkToStoragePtr HTTP::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    bool has_wildcards = write_url.find(PartitionedSink::PARTITION_ID_WILDCARD) != String::npos;
    const auto * insert_query = dynamic_cast<const ASTInsertQuery *>(query.get());
    ASTPtr partition_by_ast = nullptr;
    if (insert_query != nullptr)
        partition_by_ast = insert_query->partition_by ? insert_query->partition_by : partition_by;

    bool is_partitioned_implementation = partition_by_ast && has_wildcards;

    HTTPConfiguration config{
        .uri = write_url,
        .http_method = write_method,
        .headers = headers,
        .ca_cert_file = ca_file,
        .client_key_file = key_file,
        .insecure_connection = skip_ssl_verification,
        .format = dataFormat(),
        .format_settings = getFormatSettings(query_context),
        .compression_method = compression_method,
        .use_chunked_transfer_encoding = settings->use_chunked_encoding.value,
        .timeouts = timeouts.copyAndUpdateHTTPTimeouts(query_context->getSettingsRef())};

    if (is_partitioned_implementation)
    {
        return std::make_shared<PartitionedHTTPSink>(
            partition_by_ast,
            std::move(config),
            metadata_snapshot->getSampleBlock(),
            batch_config.copyAndUpdateFromContext(query_context),
            query_context);
    }
    else
    {
        return std::make_shared<HTTPSink>(
            std::move(config), metadata_snapshot->getSampleBlock(), batch_config.copyAndUpdateFromContext(query_context), query_context);
    }
}

}

}
