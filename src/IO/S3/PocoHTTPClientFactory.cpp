#include "config.h"

#if USE_AWS_MSK_IAM || USE_AWS_S3 /// proton: updated

#include "PocoHTTPClientFactory.h"

#include <IO/S3/PocoHTTPClient.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpRequest.h>

namespace DB::S3
{
std::shared_ptr<Aws::Http::HttpClient>
PocoHTTPClientFactory::CreateHttpClient(const Aws::Client::ClientConfiguration & client_configuration) const
{
    /// proton: starts. the prefix comes from PROJECT_NAME in CMakeLists.txt:3, we may rename later.
    using namespace std::literals;
    static constexpr std::array prefixes{"Timeplus"sv, "proton"sv, "Proton"sv};
    const std::string_view ua = client_configuration.userAgent;
    if (std::ranges::any_of(prefixes, [&](auto p) { return ua.starts_with(p); }))
        return std::make_shared<PocoHTTPClient>(static_cast<const PocoHTTPClientConfiguration &>(client_configuration));
    /// proton: ends.
    else /// This client is created inside the AWS SDK with default settings to obtain ECS credentials from localhost.
        return std::make_shared<PocoHTTPClient>(client_configuration);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::String & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory & streamFactory) const
{
    return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> PocoHTTPClientFactory::CreateHttpRequest(
    const Aws::Http::URI & uri, Aws::Http::HttpMethod method, const Aws::IOStreamFactory &) const
{
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("PocoHTTPClientFactory", uri, method);

    /// Don't create default response stream. Actual response stream will be set later in PocoHTTPClient.
    request->SetResponseStreamFactory(null_factory);

    return request;
}

}

#endif
