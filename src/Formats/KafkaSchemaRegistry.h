#pragma once

#include <IO/ReadBuffer.h>
#include <Poco/URI.h>

namespace DB
{

/// A helper class helps working with Kafka schema registry.
class KafkaSchemaRegistry final
{
public:
    static KafkaSchemaRegistry & instance()
    {
        static KafkaSchemaRegistry ret {};
        return ret;
    }

    UInt32 readSchemaId(ReadBuffer & in);
    String fetchSchema(const Poco::URI & base_url, UInt32 id, const String & username = "", const String & password = "");
};

}
