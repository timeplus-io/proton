#include <IO/Kafka/AwsMskIamSigner.h>

#if USE_AWS_MSK_IAM

#    include "config_version.h"

#    include <IO/S3/Client.h>
#    include <IO/S3/Credentials.h>
#    include <Common/Base64.h>
#    include <Common/Exception.h>
#    include <Common/logger_useful.h>

#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/auth/signer/AWSAuthV4Signer.h>
#    include <aws/core/http/URI.h>
#    include <aws/core/http/standard/StandardHttpRequest.h>

#    include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
extern const int AWS_ERROR;
}

namespace Kafka
{

namespace
{

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> pickCredentialsProvider(const String & region)
{
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;

    auto client_config = S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/region,
        /*remote_host_filter=*/{},
        /*s3_max_redirects=*/10,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*get_request_throttler=*/nullptr,
        /*put_request_throttler=*/nullptr);

    S3::S3CredentialsProviderChain provider_chain{
        /*configuration=*/client_config,
        /*credentials=*/{},
        /*credentials_configuration=*/{.use_environment_credentials = true}};

    for (size_t i = 0; const auto & cp : provider_chain.GetProviders())
    {
        try
        {
            const auto creds = cp->GetAWSCredentials();
            if (!creds.IsEmpty())
            {
                LOG_INFO(&Poco::Logger::get("AWS_MSK_IAM"), "The {}th provider from the provider chain has been picked", i);
                provider = cp;
                break;
            }
            ++i;
        }
        catch (...)
        {
            /// Just skip to the next one
        }
    }

    if (!provider)
        throw Exception(ErrorCodes::AWS_ERROR, "No AWS credentials available");

    return provider;
}

}

AwsMskIamSinger::AwsMskIamSinger(const std::string & region, int64_t expiration_seconds_)
    : url(fmt::format("https://kafka.{}.amazonaws.com/", region)), expiration_seconds(expiration_seconds_)
{
    aws_credentials = pickCredentialsProvider(region);
    aws_auth_signer = std::make_shared<const Aws::Client::AWSAuthV4Signer>(aws_credentials, "kafka-cluster", region);
}

AwsMskIamSinger::Token AwsMskIamSinger::generateToken() const
{
    Aws::Http::URI uri{url};
    uri.AddQueryStringParameter("Action", "kafka-cluster:Connect");
    Aws::Http::Standard::StandardHttpRequest req(uri, Aws::Http::HttpMethod::HTTP_GET);

    if (!aws_auth_signer->PresignRequest(req, expiration_seconds))
    {
        throw Exception(ErrorCodes::AWS_ERROR, "Failed to pre-sign request for generating MSK IAM token");
    }

    Token result;

    auto presigned_uri = req.GetUri();
    auto query_params = presigned_uri.GetQueryStringParameters();

    auto sign_date_param = query_params.find("X-Amz-Date");
    chassert(sign_date_param != query_params.end());
    Aws::Utils::DateTime sign_date{sign_date_param->second, Aws::Utils::DateFormat::ISO_8601_BASIC};

    result.expiration_ms = sign_date.Millis() + expiration_seconds * 1000;

    static const std::string user_agent = fmt::format("{}/{}", VERSION_NAME, VERSION_STRING);
    presigned_uri.AddQueryStringParameter("User-Agent", user_agent);

    auto presigned_uri_str = presigned_uri.GetURIString();
    result.token = base64Encode(presigned_uri_str, /*url_encoding=*/true, /*no_padding=*/true);

    return result;
}

}

}

#endif
