#pragma once

#include <Common/SipHash.h>
#include <IO/ReadBuffer.h>

#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>

namespace DB
{

/// A helper class helps working with Kafka schema registry.
class KafkaSchemaRegistry final
{
public:
    static UInt32 readSchemaId(ReadBuffer & in);

    /// The key type for caching KafkaSchemaRegistry
    struct CacheKey
    {
        String base_url;
        String credentials;
        String private_key_file;
        String certificate_file;
        String ca_location;
        bool skip_cert_check;

        bool operator==(const CacheKey & rhs) const
        {
            return std::tie(base_url, credentials, private_key_file, certificate_file, ca_location, skip_cert_check)
                == std::tie(rhs.base_url, rhs.credentials, rhs.private_key_file, rhs.certificate_file, rhs.ca_location, rhs.skip_cert_check);
        }
    };

    /// The hash function for caching KafkaSchemaRegistry
    struct CacheHasher
    {
        size_t operator()(const CacheKey & k) const
        {
            SipHash s;
            s.update(k.base_url);
            s.update(k.credentials);
            s.update(k.private_key_file);
            s.update(k.certificate_file);
            s.update(k.ca_location);
            s.update(k.skip_cert_check);
            return s.get64();
        }
    };

    /// \param credentials_ is expected to be formatted in "<username>:<password>".
    KafkaSchemaRegistry(
        const String & base_url_,
        const String & credentials_,
        const String & private_key_file_,
        const String & certificate_file_,
        const String & ca_location_,
        bool skip_cert_check);

    String fetchSchema(UInt32 id);

private:
    Poco::URI base_url;
    Poco::Net::HTTPBasicCredentials credentials;
    String private_key_file;
    String certificate_file;
    String ca_location;
    Poco::Net::Context::VerificationMode Verification_mode;

    Poco::Logger* logger;
};

}
