#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <Interpreters/Context.h>

namespace DB
{

namespace ExternalStream
{

void IcebergS3Configuration::createClient(const ContextPtr & ctx)
{
    if (client)
        return;

    request_settings.updateFromSettings(ctx->getSettings());

    DB::S3::PocoHTTPClientConfiguration client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
        auth_settings.region,
        ctx->getRemoteHostFilter(),
        static_cast<unsigned>(ctx->getGlobalContext()->getSettingsRef().s3_max_redirects),
        ctx->getGlobalContext()->getSettingsRef().enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler);

    /// Using "https://s3.amazonaws.com" will cause some issues on some API, like HeadBucket.
    /// No need to override the endpoint in this case.
    if (url.endpoint != "https://s3.amazonaws.com")
        client_configuration.endpointOverride = url.endpoint;
    client_configuration.maxConnections = static_cast<unsigned>(request_settings.max_connections);

    auto headers = auth_settings.headers;
    if (!headers_from_ast.empty())
        headers.insert(headers.end(), headers_from_ast.begin(), headers_from_ast.end());

    client = DB::S3::ClientFactory::instance().create(
        client_configuration,
        url.is_virtual_hosted_style,
        auth_settings.access_key_id,
        auth_settings.secret_access_key,
        auth_settings.server_side_encryption_customer_key_base64,
        auth_settings.server_side_encryption_kms_config,
        std::move(headers),
        S3::CredentialsConfiguration{
            auth_settings.use_environment_credentials.value_or(true),
            auth_settings.use_insecure_imds_request.value_or(ctx->getConfigRef().getBool("s3.use_insecure_imds_request", false)),
            auth_settings.expiration_window_seconds.value_or(
                ctx->getConfigRef().getUInt64("s3.expiration_window_seconds", S3::DEFAULT_EXPIRATION_WINDOW_SECONDS)),
            auth_settings.no_sign_request.value_or(ctx->getConfigRef().getBool("s3.no_sign_request", false)),
        },
        auth_settings.session_token);
}

}

}

#endif
