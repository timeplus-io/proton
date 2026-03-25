#pragma once

#include "config.h"

#if USE_AVRO

#    include <Formats/KafkaSchemaRegistry.h>
#    include <Compiler.hh>
#    include <ValidSchema.hh>
#    include <Common/CacheBase.h>
#    include <Formats/FormatSettings.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

class KafkaSchemaRegistryForAvro final
{
public:
    /// Tries to get an existing KafkaSchemaRegistryForAvro instance that matches the format_settings, if there is none, create a new one.
    static std::shared_ptr<KafkaSchemaRegistryForAvro> getOrCreate(const FormatSettings & format_settings);

    KafkaSchemaRegistryForAvro(
        const std::string & base_url,
        const std::string & credentials,
        const std::string & private_key_file,
        const std::string & certificate_file,
        const std::string & ca_location,
        bool skip_cert_check,
        size_t schema_cache_max_size = 1000);

    KafkaSchemaRegistryForAvro(const KafkaSchemaRegistryForAvro &) = delete;
    KafkaSchemaRegistryForAvro & operator=(const KafkaSchemaRegistryForAvro &) = delete;

    avro::ValidSchema getSchema(UInt32 id);
    /// Returns the schema ID and schema for a subject.
    std::pair<UInt32, avro::ValidSchema> getSchemaForSubject(const String & subject_name, bool force_refresh = false);

private:
    avro::ValidSchema fetchAndCompileSchema(UInt32 id);
    std::pair<UInt32, avro::ValidSchema> fetchAndCompileSchemaForSubject(const String & subject_name);

    KafkaSchemaRegistry registry;
    CacheBase<UInt32, avro::ValidSchema> schema_cache;
    CacheBase<String, UInt32> subject_schema_cache;
};

}

#endif
