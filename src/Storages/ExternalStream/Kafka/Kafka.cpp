#include <Cluster/Common/Constants.h>
#include <Cluster/Common/LogTrack.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/KafkaSink.h>
#include <Storages/ExternalStream/Kafka/KafkaSource.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/parseShards.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include "Storages/ExternalStream/StorageExternalStreamImpl.h"
#include <Formats/FormatSettings.h>
#include <Formats/KafkaSchemaRegistryForAvro.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <filesystem>
#include <mutex>
#include <optional>
#include <ranges>
#include <string_view>

namespace DB
{

namespace ErrorCodes
{
extern const int ABORTED;
extern const int ILLEGAL_COLUMN;
extern const int INVALID_CONFIG_PARAMETER;
extern const int INVALID_SETTING_VALUE;
extern const int NO_AVAILABLE_KAFKA_CONSUMER;
}

namespace
{

/// Checks if a config is a unsupported global config, i.e. the config is not supposed
/// to be configured by users.
bool isUnsupportedGlobalConfig(const String & name)
{
    static std::set<String> global_configs{
        "builtin.features",
        "metadata.broker.list",
        "bootstrap.servers",
        "enabled_events",
        "error_cb",
        "throttle_cb",
        "stats_cb",
        "log_cb",
        "log.queue",
        "enable.random.seed",
        "background_event_cb",
        "socket_cb",
        "connect_cb",
        "closesocket_cb",
        "open_cb",
        "resolve_cb",
        "opaque",
        "default_topic_conf",
        "internal.termination.signal",
        "api.version.request",
        "security.protocol",
        "ssl_key", /// requires dedicated API
        "ssl_certificate", /// requires dedicated API
        "ssl_ca", /// requires dedicated API
        "ssl_engine_callback_data",
        "ssl.certificate.verify_cb",
        "sasl.mechanisms",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "oauthbearer_token_refresh_cb",
        "plugin.library.paths",
        "interceptors",
        "group.id",
        "group.instance.id",
        "enable.auto.commit",
        "enable.auto.offset.store",
        "consume_cb",
        "rebalance_cb",
        "offset_commit_cb",
        "enable.partition.eof",
        "dr_cb",
        "dr_msg_cb",
    };

    return global_configs.contains(name);
}

/// Checks if a config a unsupported topic config.
bool isUnsupportedTopicConfig(const String & name)
{
    static std::set<String> topic_configs{
        /// producer
        "partitioner_cb",
        "msg_order_cmp",
        "produce.offset.report",
        /// both
        "opaque",
        "auto.commit.enable",
        "enable.auto.commit",
        "auto.commit.interval.ms",
        "auto.offset.reset",
        "offset.store.path",
        "offset.store.sync.interval.ms",
        "offset.store.method",
    };

    return topic_configs.contains(name);
}

bool isUnsupportedConfig(const String & name)
{
    return isUnsupportedGlobalConfig(name) || isUnsupportedTopicConfig(name);
}

DB::Kafka::Conf createConfFromSettings(const KafkaExternalStreamSettings & settings)
{
    if (settings.brokers.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `brokers` setting for kafka external stream");

    DB::Kafka::Conf conf;

    if (!settings.region.value.empty())
        conf.setRegion(settings.region);

    /// 1. Set default values
    /// -- For Producer
    conf.set("enable.idempotence", "true");
    conf.set("message.timeout.ms", "0" /* infinite */);
    /// -- For Consumer
    /// If the desired offset is out of range, read from the beginning to avoid data lost.
    conf.set("auto.offset.reset", "earliest");

    /// 2. Process the `properties` setting. The value of `properties` looks like this:
    /// 'message.max.bytes=1024;max.in.flight=1000;group.id=my-group'
    std::vector<String> parts;
    boost::split(parts, settings.properties.value, boost::is_any_of(";"));

    for (const auto & part : parts)
    {
        /// skip empty part, this happens when there are redundant / trailing ';'
        if (unlikely(std::ranges::all_of(part, [](char ch) { return isspace(static_cast<unsigned char>(ch)); })))
            continue;

        auto equal_pos = part.find('=');
        if (unlikely(equal_pos == std::string::npos || equal_pos == 0 || equal_pos == part.size() - 1))
            throw DB::Exception(DB::ErrorCodes::INVALID_SETTING_VALUE, "Invalid property `{}`, expected format: <key>=<value>.", part);

        auto key = part.substr(0, equal_pos);
        auto value = part.substr(equal_pos + 1);

        /// no spaces are supposed be around `=`, thus only need to
        /// remove the leading spaces of keys and trailing spaces of values
        boost::trim_left(key);
        boost::trim_right(value);

        if (isUnsupportedConfig(key))
            throw DB::Exception(DB::ErrorCodes::INVALID_SETTING_VALUE, "Unsupported property {}", key);

        conf.set(key, value);
    }

    /// 3. Handle the specific settings have higher priority
    conf.setBrokers(settings.brokers.value);

    conf.set("security.protocol", settings.security_protocol.value);

    if (settings.usesSASL())
    {
        auto sasl_mechanism = settings.sasl_mechanism.value;
        conf.setSaslMechanism(sasl_mechanism.empty() ? "PLAIN" : sasl_mechanism);
        if (!settings.username.value.empty())
            conf.set("sasl.username", settings.username.value);
        if (!settings.password.value.empty())
            conf.set("sasl.password", settings.password.value);
    }

    if (settings.usesSecureConnection())
    {
        conf.set("enable.ssl.certificate.verification", settings.skip_ssl_cert_check ? "false" : "true");
        if (!settings.ssl_ca_cert_file.value.empty())
            conf.set("ssl.ca.location", settings.ssl_ca_cert_file.value);
    }

    return conf;
}

const std::vector<TypeIndex> raw_message_key_types{
    TypeIndex::Bool, TypeIndex::UInt8, TypeIndex::UInt16, TypeIndex::UInt32, TypeIndex::UInt64,
    TypeIndex::Int8, TypeIndex::Int16, TypeIndex::Int32, TypeIndex::Int64,
    TypeIndex::Float32, TypeIndex::Float64, TypeIndex::String, TypeIndex::FixedString,
};

const std::vector<TypeIndex> avro_message_key_types{TypeIndex::String, TypeIndex::FixedString};

void validateMessageKeyColumnType(const DataTypePtr & type, const std::vector<TypeIndex> & allowed)
{
    if (type->isNullable())
        validateMessageKeyColumnType(
            static_cast<const DataTypeNullable &>(*type).getNestedType(), allowed);
    else if (std::ranges::none_of(allowed, [&](auto t) { return t == type->getTypeId(); }))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_key` column does not support type {}", type->getName());
}

void validateMessageHeadersColumnType(const DataTypePtr & type)
{
    if (!WhichDataType{type}.isMap())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_headers` column must have type of map(string, string)");

    const auto & map_type = dynamic_cast<const DataTypeMap &>(*type);
    if (!WhichDataType{map_type.getKeyType()}.isStringOrFixedString() || !WhichDataType{map_type.getValueType()}.isStringOrFixedString())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_headers` column must have type of map(string, string)");
}

}

namespace ExternalStream
{

DB::Kafka::Conf Kafka::createConf(KafkaExternalStreamSettings settings_)
{
    if (const auto & ca_pem = settings_.ssl_ca_pem.value; !ca_pem.empty())
    {
        createTempDirIfNotExists();
        broker_ca_file = tmpdir / "broker_ca.pem";
        WriteBufferFromFile wb{broker_ca_file};
        wb.write(ca_pem.data(), ca_pem.size());
        settings_.ssl_ca_cert_file = broker_ca_file;
    }
    return createConfFromSettings(settings_);
}

Kafka::Kafka(
    StorageID storage_id,
    StorageInMemoryMetadata storage_metadata_,
    std::unique_ptr<ExternalStreamSettings> settings_,
    ASTs engine_args_,
    bool /*attach*/,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr context)
    : StorageExternalStreamImpl(std::move(storage_id), storage_metadata_, std::move(settings_), context)
    , engine_args(std::move(engine_args_))
    , external_stream_counter(std::move(external_stream_counter_))
    , poll_timeout_ms(settings->poll_waittime_ms.value)
{
    assert(external_stream_counter);

    const auto & columns = getInMemoryMetadataPtr()->getColumns();
    const bool has_event_time = columns.has(ProtonConsts::RESERVED_EVENT_TIME);
    const bool has_message_key = columns.has(ProtonConsts::RESERVED_MESSAGE_KEY);
    const bool has_message_headers = columns.has(ProtonConsts::RESERVED_MESSAGE_HEADERS);

    if (has_event_time || has_message_key || has_message_headers)
        settings->set("one_message_per_row", true);

    if (has_message_key && !settings->message_key_schema_name.value.empty())
    {
        FormatSettings key_format_settings = getFormatSettings(context);
        key_format_settings.kafka_schema_registry.subject_name = settings->message_key_schema_name.value;
        avro_key_schema_registry = KafkaSchemaRegistryForAvro::getOrCreate(key_format_settings);
    }

    cacheVirtualColumnNamesAndTypes();

    auto conf = createConf(settings->getKafkaSettings());
    conf.setLogCallback(&Kafka::onLog);
    conf.setErrorCallback(&Kafka::onError);
    conf.setThrottleCallback(&Kafka::onThrottle);
    conf.setDrMsgCallback(&KafkaSink::onMessageDelivery);

    if (settings->log_stats)
        conf.setStatsCallback(&Kafka::onStats);
    else
        conf.setStatsCallback([](struct rd_kafka_s *, char *, size_t, void *) { return 0; });

    if (auto topic_refresh_interval_ms_value = conf.get("topic.metadata.refresh.interval.ms"))
        topic_refresh_interval_ms = std::stoi(*topic_refresh_interval_ms_value);
    else
        topic_refresh_interval_ms = 300'000;

    /// Atomic store paired with the atomic_load_explicit in getClient(): even
    /// though no other thread can see this Kafka instance during construction,
    /// keeping store/load symmetric on `client` makes the data race analyzable
    /// and avoids subtle re-ordering surprises if the constructor is later
    /// inlined in a way that lets the publication of `this` race the assignment.
    std::atomic_store_explicit(
        &client,
        DB::Kafka::ConnectionFactory::instance().getConnection(std::move(conf)),
        std::memory_order_release);
}

DB::Kafka::ConnectionPtr Kafka::getClient() const
{
    /// `client` may be reset to nullptr by shutdown() concurrently with reads
    /// from materialized-view pipelines. Load atomically — std::shared_ptr's
    /// own copy/assign is not atomic w.r.t. concurrent reset(), so a plain
    /// `client->...` access is racy and crashes inside getConsumer when
    /// shutdown has just nulled the member (SIGSEGV at offset 0x50, see
    /// tests/external_stream/kafka_external_stream.md).
    auto local_client = std::atomic_load_explicit(&client, std::memory_order_acquire);
    if (!local_client)
        throw Exception(ErrorCodes::ABORTED, "Kafka external stream is shutting down");
    return local_client;
}

void Kafka::startup()
{
    StorageExternalStreamImpl::startup();
    LOG_INFO(logger, "Starting Kafka External Stream");
}

void Kafka::shutdown(bool /*dropping*/)
{
    LOG_INFO(logger, "Shutting down Kafka External Stream");

    /// Release all resources here rather than relying on the deconstructor.
    /// Because the `Kafka` instance will not be destroyed immediately when the external stream gets dropped.
    /// Atomic store paired with atomic_load_explicit in getClient() — without atomic
    /// publication of the empty pointer, a concurrent reader can observe a torn
    /// or partially-reset shared_ptr and crash inside Connection::getConsumer.
    std::atomic_store_explicit(&client, DB::Kafka::ConnectionPtr{}, std::memory_order_release);

    tryRemoveTempDir();
}

bool Kafka::hasCustomShardingExpr() const
{
    if (engine_args.empty())
        return false;

    if (auto * shard_func = shardingExprAst()->as<ASTFunction>())
        return !boost::iequals(shard_func->name, "rand");

    return true;
}

NamesAndTypesList Kafka::getVirtuals() const
{
    return virtual_column_names_and_types;
}

std::optional<String> Kafka::preferredColumn() const
{
    return ProtonConsts::RESERVED_EVENT_SEQUENCE_ID;
}

void Kafka::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_KEY, std::make_shared<DataTypeString>()));

    DataTypes header_types{/*key_type*/ std::make_shared<DataTypeString>(), /*value_type*/ std::make_shared<DataTypeString>()};
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_HEADERS, std::make_shared<DataTypeMap>(header_types)));
}

std::vector<Int64> Kafka::getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<uint64_t> & shards_to_query) const
{
    assert(seek_to_info);
    seek_to_info->replicateForShards(static_cast<uint32_t>(shards_to_query.size()));
    if (!seek_to_info->isTimeBased())
    {
        return seek_to_info->getSeekPoints();
    }
    else
    {
        std::vector<DB::Kafka::PartitionTimestamp> partition_timestamps;
        partition_timestamps.reserve(shards_to_query.size());
        auto seek_timestamps{seek_to_info->getSeekPoints()};
        assert(shards_to_query.size() == seek_timestamps.size());

        for (auto [shard, timestamp] : std::ranges::views::zip(shards_to_query, seek_timestamps))
            partition_timestamps.emplace_back(shard, timestamp);

        return getClient()->getOffsetsForTimestamps(settings->topic.value, partition_timestamps);
    }
}

void Kafka::validateSettings(const ExternalStreamSettingsPtr & new_settings, bool /*change_settings*/, const ContextPtr & /*context_*/) const
{
    chassert(new_settings->type.value == StreamTypes::KAFKA || new_settings->type.value == StreamTypes::REDPANDA);

    if (new_settings->topic.value.empty())
    {
        LOG_ERROR(logger, "Setting `topic` is empty");
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", new_settings->type.value);
    }

    if (!new_settings->message_key.value.empty())
    {
        LOG_ERROR(logger, "Setting `message_key` is deprecated, it won't be used");
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Setting `message_key` is deprecated, define the _tp_message_key column instead");
    }

    if (!new_settings->kafka_schema_registry_url.value.empty())
    {
        const auto & format = new_settings->data_format.value;
        const bool format_supported = format == "ProtobufSingle" || format == "Avro";
        const bool key_uses_registry = !new_settings->message_key_schema_name.value.empty();
        /// The schema registry URL is valid if either the message body format requires it
        /// (Avro/ProtobufSingle) or the message key is Avro-encoded via `message_key_schema_name`.
        if (!format_supported && !key_uses_registry)
        {
            LOG_ERROR(
                logger,
                "Kafka external stream with schema registry only supports 'ProtobufSingle' or 'Avro' data formats, "
                "or `message_key_schema_name` for Avro-encoded keys: actual='{}'",
                format);

            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Kafka external stream with schema registry only supports 'ProtobufSingle' or 'Avro' data formats, "
                "or `message_key_schema_name` for Avro-encoded keys");
        }
    }

    if (!new_settings->message_key_schema_name.value.empty() && new_settings->kafka_schema_registry_url.value.empty())
    {
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "`message_key_schema_name` is only supported when `kafka_schema_registry_url` is set");
    }

    const auto & columns = getInMemoryMetadataPtr()->getColumns();
    const bool has_event_time = columns.has(ProtonConsts::RESERVED_EVENT_TIME);
    const bool has_message_key = columns.has(ProtonConsts::RESERVED_MESSAGE_KEY);
    const bool has_message_headers = columns.has(ProtonConsts::RESERVED_MESSAGE_HEADERS);

    if (has_event_time)
    {
        LOG_WARNING(
            logger,
            "Column `{}` is a reserved virtual column for Kafka/Redpanda external streams and is no longer supported as a physical column. "
            "It will be ignored in payload parsing and treated as transport metadata.",
            ProtonConsts::RESERVED_EVENT_TIME);

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Column `{}` is a reserved virtual column for Kafka/Redpanda external streams and cannot be defined as a physical column",
            ProtonConsts::RESERVED_EVENT_TIME);
    }

    if (has_event_time || has_message_key || has_message_headers)
    {
        if (new_settings->isChanged("one_message_per_row") && !new_settings->one_message_per_row)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`one_message_per_row` cannot be set to `false` when the `{}` / `{}` / `{}` column is defined",
                ProtonConsts::RESERVED_EVENT_TIME,
                ProtonConsts::RESERVED_MESSAGE_KEY,
                ProtonConsts::RESERVED_MESSAGE_HEADERS);
    }
}

void Kafka::validateColumns() const
{
    const auto & columns = getInMemoryMetadataPtr()->getColumns();
    const bool has_message_key = columns.has(ProtonConsts::RESERVED_MESSAGE_KEY);
    const bool has_message_headers = columns.has(ProtonConsts::RESERVED_MESSAGE_HEADERS);

    if (has_message_headers)
        validateMessageHeadersColumnType(columns.getColumn({GetColumnsOptions::Kind::All}, ProtonConsts::RESERVED_MESSAGE_HEADERS).type);

    if (has_message_key)
    {
        validateMessageKeyColumnType(
            columns.getColumn({GetColumnsOptions::Kind::All}, ProtonConsts::RESERVED_MESSAGE_KEY).type,
            settings->message_key_schema_name.value.empty() ? raw_message_key_types : avro_message_key_types);

        if (hasCustomShardingExpr())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`sharding_expr` cannot be set when the `{}` column is defined",
                ProtonConsts::RESERVED_MESSAGE_KEY);
    }
}

void Kafka::validate(const ContextPtr & context) const
{
    validateSettings(settings, false, context);
    validateColumns();

    if (getClient()->getPartitionCount(topicName(), settings->connection_timeout_ms.value.totalMilliseconds()) < 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Topic has no partitions, topic={}", topicName());
}

std::optional<UInt64> Kafka::totalRows(const Settings & settings_ref) const
{
    /// Only optimize trivial count when one message represents one row.
    if (!settings->one_message_per_row.value)
        return {};

    const auto connection_timeout_ms = settings->connection_timeout_ms.value.totalMilliseconds();

    /// Hoist getClient() out of the per-shard loop: one atomic_load + null-check
    /// for the whole totalRows() call, and the local `local_client` keeps the
    /// Connection alive even if shutdown() reset the member mid-loop.
    auto local_client = getClient();
    auto shards_to_query = parseQueryShards(
        settings_ref.shards.value, local_client->getPartitionCount(topicName(), connection_timeout_ms));
    LOG_INFO(logger, "Counting number of messages topic={} partitions=[{}]", topicName(), fmt::join(shards_to_query, ","));

    UInt64 rows = 0;
    for (auto shard : shards_to_query)
    {
        auto marks = local_client->getConsumer(topicName())->queryWatermarkOffsets(static_cast<Int32>(shard), connection_timeout_ms);
        LOG_INFO(logger, "Watermark offsets topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
        rows += marks.high - marks.low;
    }
    return rows;
}

std::vector<int64_t> Kafka::getLastSNs() const
{
    /// Hoist getClient() out of the per-partition loop. local_client keeps
    /// the Connection alive across the whole sweep even if shutdown() races.
    auto local_client = getClient();
    auto partitions = local_client->getPartitionCount(topicName(), settings->connection_timeout_ms.value.totalMilliseconds());

    std::vector<int64_t> result;
    result.reserve(partitions);

    for (int32_t i = 0; i < partitions; ++i)
    {
        auto offset = local_client->getWatermarkOffsets(topicName(), i);
        result.push_back(std::max(offset.high - 1, offset.low));
    }

    return result;
}

Pipe Kafka::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    const auto connection_timeout_ms = static_cast<UInt64>(settings->connection_timeout_ms.value.totalMilliseconds());

    /// The consumer can be shared between all the sources in the same pipe, because each source reads from a different partition.
    auto consumer = getClient()->getConsumer(topicName());

    /// User can explicitly consume specific kafka partitions by specifying `shards=` setting
    /// `SELECT * FROM kafka_stream SETTINGS shards=0,3`
    auto shards_to_query = parseQueryShards(context->getSettingsRef().shards.value, consumer->getPartitionCount(connection_timeout_ms));
    chassert(!shards_to_query.empty());

    auto streaming = query_info.isStreaming();

    LOG_INFO(
        logger,
        "Reading topic={} partitions=[{}] streaming={} consumer={}",
        topicName(),
        fmt::join(shards_to_query, ","),
        streaming,
        consumer->name());

    Pipes pipes;
    pipes.reserve(shards_to_query.size());

    {
        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_SEQUENCE_ID});

        auto seek_to_info = query_info.seek_to_info;
        /// seek_to defaults to 'latest' for streaming. In non-streaming case, 'earliest' is preferred.
        if (!streaming && seek_to_info->getSeekTo().empty())
            seek_to_info = std::make_shared<SeekToInfo>("earliest");

        auto offsets = getOffsets(seek_to_info, shards_to_query);
        assert(offsets.size() == shards_to_query.size());

        consumer->initialize(shards_to_query);

        auto format_settings = getFormatSettings(context);

        for (auto [shard, offset] : std::ranges::views::zip(shards_to_query, offsets))
        {
            std::optional<Int64> high_watermark = std::nullopt;
            if (!streaming)
            {
                auto marks = consumer->queryWatermarkOffsets(static_cast<Int32>(shard), connection_timeout_ms);
                LOG_INFO(logger, "Watermarks topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
                high_watermark = marks.high;

                if (marks.low == marks.high) /// there are no messages in the topic
                {
                    /// As there are no messages, no need to create a KafkaSource instance at all.
                    pipes.emplace_back(std::make_shared<NullSource>(header));
                    continue;
                }
                else if (offset >= 0 && offset < marks.low) /// if offset < marks.low, consuming will stuck
                    offset = marks.low;
                else if (offset == cluster::Constants::LatestSN || offset > marks.high)
                    offset = marks.high;
            }
            pipes.emplace_back(std::make_shared<KafkaSource>(
                header,
                storage_snapshot,
                dataFormat(),
                format_settings,
                topicName(),
                consumer,
                shard,
                offset,
                high_watermark,
                max_block_size,
                KafkaSource::Timeouts{
                    .connection_timeout_ms = connection_timeout_ms,
                    .consumer_stall_timeout_ms = static_cast<UInt64>(settings->consumer_stall_timeout_ms.totalMilliseconds()),
                },
                avro_key_schema_registry,
                external_stream_counter,
                context,
                logger));
        }
    }

    LOG_INFO(
        logger,
        "Starting reading {} streams by seeking to {} with {} in dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo(),
        consumer->name());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = context->getSettingsRef().min_threads.value;
    if (min_threads > shards_to_query.size())
        pipe.resize(min_threads);

    return pipe;
}

SinkToStoragePtr Kafka::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    if (hasSchemaRegistryUrl() && data_format == "ProtobufSingle")
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Write Protobuf data with schema registry is not supported");

    /// Encoding _tp_message_key as Avro binary (Confluent wire format) on write is not yet implemented.
    /// Currently only decoding Avro-encoded keys on read is supported. When this is implemented,
    /// the sink will need to: fetch the schema from the registry, serialize the key JSON string
    /// into a GenericDatum, binary-encode it, and prepend the Confluent wire header (magic byte + schema ID).
    if (avro_key_schema_registry)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Writing Avro-encoded message keys via schema registry is not yet supported. "
            "`message_key_schema_name` is currently read-only. "
            "To write a plain-text message key, omit `message_key_schema_name` and insert a string into `_tp_message_key` directly.");

    auto producer = getClient()->getProducer(topicName());

    auto sink = std::make_shared<KafkaSink>(
        *this,
        metadata_snapshot->getSampleBlock(),
        producer,
        settings->connection_timeout_ms.value.totalMilliseconds(),
        /*refresh_topic_partitions=*/context->isQueryFromMaterializedView(),
        external_stream_counter,
        getLogger(fmt::format("{}.{}", getLoggerName(), producer->name())),
        context);

    producer->start(/*need_poll=*/true);
    return sink;
}

int Kafka::onStats(struct rd_kafka_s * rk, char * json, size_t json_len, void * /*opaque*/)
{
    std::string s(json, json + json_len);
    /// controlled by the `statistics.interval.ms` property, which by default is `0`, meaning no stats
    LOG_INFO(cbLogger(), "stats of {}: {}", rd_kafka_name(rk), s);
    return 0;
}

void Kafka::onLog(const struct rd_kafka_s * rk, int level, const char * fac, const char * buf)
{
    if (level < 4)
        LOG_ERROR(cbLogger(), "{}|{} {}", rd_kafka_name(rk), fac, buf);
    else if (level == 4)
    {
        String msg{buf};
        /// For simplicity, we use use one conf object for both consumer and producer,
        /// thus we don't care about such mis-match propery warnings.
        if (!msg.contains("property and will be ignored by this"))
            LOG_WARNING(cbLogger(), "{}|{} {}", rd_kafka_name(rk), fac, buf);
    }
    else
        LOG_INFO(cbLogger(), "{}|{} {}", rd_kafka_name(rk), fac, buf);
}

namespace
{
std::pair<bool, uint64_t> shouldLogKafkaError(uint64_t log_key)
{
    static std::mutex mu;
    static cluster::LogTrackContainer tracked_logs;

    std::lock_guard lock{mu};
    /// Client names like `rdkafka#consumer-N` are never reused within a process, so stale
    /// keys accumulate; reset the container instead of letting it grow unboundedly.
    if (tracked_logs.size() > 10000)
        tracked_logs.clear();

    return cluster::shouldLog(tracked_logs, log_key, /*throttling_sec=*/30);
}
}

void Kafka::onError(struct rd_kafka_s * rk, int err, const char * reason, void * /*opaque*/)
{
    if (err == RD_KAFKA_RESP_ERR__FATAL)
    {
        char errstr[512] = {'\0'};
        rd_kafka_fatal_error(rk, errstr, sizeof(errstr));
        LOG_ERROR(cbLogger(), "Fatal error found on {}, error={}", rd_kafka_name(rk), errstr);
    }
    else
    {
        /// During a broker outage librdkafka reports transport failures many times per second
        /// on every client; throttle per (client, error code) to keep the server log readable.
        auto log_key = std::hash<std::string_view>{}(rd_kafka_name(rk)) ^ static_cast<uint64_t>(err);
        if (auto [should_log, log_count] = shouldLogKafkaError(log_key); should_log)
            LOG_WARNING(
                cbLogger(),
                "Error occurred on {}, error={}, reason={}, log_recurring={}",
                rd_kafka_name(rk),
                rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)),
                reason,
                log_count);
    }
}

void Kafka::onThrottle(struct rd_kafka_s * /*rk*/, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * /*opaque*/)
{
    LOG_WARNING(cbLogger(), "Throttled on broker={}, broker_id={}, throttle_time_ms={}", broker_name, broker_id, throttle_time_ms);
}

}

}
