#pragma once

#include <IO/ReadBuffer.h>
#include <Poco/Net/HTTPBasicCredentials.h>
#include <Poco/URI.h>

namespace DB
{

/// A helper class helps working with Kafka schema registry.
class KafkaSchemaRegistry final
{
public:
    static UInt32 readSchemaId(ReadBuffer & in);

    /// `credentials_` is expected to be formatted in "<username>:<password>".
    KafkaSchemaRegistry(const String & base_url_, const String & credentials_);

    String fetchSchema(UInt32 id);

private:
    Poco::URI base_url;
    Poco::Net::HTTPBasicCredentials credentials;
};

}
