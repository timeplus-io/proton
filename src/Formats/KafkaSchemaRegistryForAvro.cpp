#include <Formats/KafkaSchemaRegistryForAvro.h>

#if USE_AVRO

namespace DB
{

namespace
{
auto & schemaRegistryCache()
{
    static CacheBase<KafkaSchemaRegistry::CacheKey, KafkaSchemaRegistryForAvro, KafkaSchemaRegistry::CacheHasher> schema_registry_cache(
        /*max_size=*/100);
    return schema_registry_cache;
}
}

std::shared_ptr<KafkaSchemaRegistryForAvro> KafkaSchemaRegistryForAvro::getOrCreate(const FormatSettings & format_settings)
{
    const auto & base_url = format_settings.kafka_schema_registry.url.empty() ? format_settings.avro.schema_registry_url
                                                                              : format_settings.kafka_schema_registry.url;

    KafkaSchemaRegistry::CacheKey key{
        base_url,
        format_settings.kafka_schema_registry.credentials,
        format_settings.kafka_schema_registry.private_key_file,
        format_settings.kafka_schema_registry.certificate_file,
        format_settings.kafka_schema_registry.ca_location,
        format_settings.kafka_schema_registry.skip_cert_check};

    auto [schema_registry, loaded] = schemaRegistryCache().getOrSet(key, [&key]() {
        return std::make_shared<KafkaSchemaRegistryForAvro>(
            key.base_url, key.credentials, key.private_key_file, key.certificate_file, key.ca_location, key.skip_cert_check);
    });
    return schema_registry;
}

KafkaSchemaRegistryForAvro::KafkaSchemaRegistryForAvro(
    const std::string & base_url,
    const std::string & credentials,
    const std::string & private_key_file,
    const std::string & certificate_file,
    const std::string & ca_location,
    bool skip_cert_check,
    size_t schema_cache_max_size)
    : registry(base_url, credentials, private_key_file, certificate_file, ca_location, skip_cert_check)
    , schema_cache(schema_cache_max_size)
    , topic_schema_cache(schema_cache_max_size)
{
}

avro::ValidSchema KafkaSchemaRegistryForAvro::getSchema(UInt32 id)
{
    auto [schema, _]
        = schema_cache.getOrSet(id, [this, id]() { return std::make_shared<avro::ValidSchema>(fetchAndCompileSchema(id)); });
    return *schema;
}

std::pair<UInt32, avro::ValidSchema> KafkaSchemaRegistryForAvro::getSchemaForTopic(const String & topic_name, bool force_refresh)
{
    auto cached_schema_id = topic_schema_cache.get(topic_name);

    if (!force_refresh && cached_schema_id)
        return {*cached_schema_id, getSchema(*cached_schema_id)};

    auto schema_and_id = fetchAndCompileSchemaForTopic(topic_name);
    schema_cache.set(schema_and_id.first, std::make_shared<avro::ValidSchema>(std::move(schema_and_id.second)));
    auto schema_id = schema_and_id.first;
    topic_schema_cache.set(topic_name, std::make_shared<UInt32>(schema_id));
    return {schema_id, getSchema(schema_id)};
}

avro::ValidSchema KafkaSchemaRegistryForAvro::fetchAndCompileSchema(UInt32 id)
{
    auto schema = registry.fetchSchema(id);
    try
    {
        return avro::compileJsonSchemaFromString(schema);
    }
    catch (const avro::Exception & e)
    {
        auto ex = Exception::createDeprecated(e.what(), ErrorCodes::INCORRECT_DATA);
        ex.addMessage(fmt::format("while fetching schema id = {}", id));
        throw std::move(ex);
    }
}

std::pair<UInt32, avro::ValidSchema> KafkaSchemaRegistryForAvro::fetchAndCompileSchemaForTopic(const String & topic_name)
{
    try
    {
        auto schema = registry.fetchLatestSchemaForTopic(topic_name);
        auto valid_schema = avro::compileJsonSchemaFromString(schema.second);
        return {schema.first, std::move(valid_schema)};
    }
    catch (const avro::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "{}: while fetching schema for topic {}", e.what(), topic_name);
    }
}

}

#endif
