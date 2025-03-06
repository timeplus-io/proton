#pragma once

#include "config.h"

#if USE_AWS_MSK_IAM

#include <string>
#include <cstdint>

namespace Aws
{
namespace Auth
{
class AWSCredentialsProvider;
}

namespace Client
{
class AWSAuthV4Signer;
}
}

namespace DB
{

namespace Kafka
{

class AwsMskIamSinger final
{
public:
    struct Token
    {
        std::string token;
        int64_t expiration_ms{0};
    };

    explicit AwsMskIamSinger(const std::string & region, int64_t expiration_seconds = 900);
    Token generateToken() const;

private:
    std::string url;
    int64_t expiration_seconds;

    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> aws_credentials;
    std::shared_ptr<const Aws::Client::AWSAuthV4Signer> aws_auth_signer;
};

}

}

#endif
