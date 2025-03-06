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

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
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
        "partitioner",
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
        if (unlikely(std::all_of(part.begin(), part.end(), [](char ch) { return isspace(static_cast<unsigned char>(ch)); })))
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

    /// 3. Handle the speicific settings have higher priority
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

void validateMessageKeyColumnType(const DataTypePtr & type)
{
    static std::vector<TypeIndex> supported_types{
        TypeIndex::Bool,
        TypeIndex::UInt8,
        TypeIndex::UInt16,
        TypeIndex::UInt32,
        TypeIndex::UInt64,
        TypeIndex::Int8,
        TypeIndex::Int16,
        TypeIndex::Int32,
        TypeIndex::Int64,
        TypeIndex::Float32,
        TypeIndex::Float64,
        TypeIndex::String,
        TypeIndex::FixedString,
        TypeIndex::Nullable,
    };
    if (std::none_of(
            supported_types.begin(), supported_types.end(), [type](auto supported_type) { return supported_type == type->getTypeId(); }))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_key` column does not support type {}", type->getName());
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
    IStorage * storage,
    std::unique_ptr<ExternalStreamSettings> settings_,
    const ASTs & engine_args_,
    StorageInMemoryMetadata & storage_metadata,
    bool attach,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr context)
    : StorageExternalStreamImpl(storage, std::move(settings_), context)
    , engine_args(engine_args_)
    , external_stream_counter(external_stream_counter_)
    , poll_timeout_ms(settings->poll_waittime_ms.value)
{
    assert(settings->type.value == StreamTypes::KAFKA || settings->type.value == StreamTypes::REDPANDA);
    assert(external_stream_counter);

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", settings->type.value);

    if (storage_metadata.getColumns().has(ProtonConsts::RESERVED_MESSAGE_KEY))
    {
        validateMessageKeyColumnType(
            storage_metadata.getColumns().getColumn({GetColumnsOptions::Kind::All}, ProtonConsts::RESERVED_MESSAGE_KEY).type);

        if (!settings->message_key.value.empty())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`message_key` cannot be set when the `{}` column is defined",
                ProtonConsts::RESERVED_MESSAGE_KEY);

        if (hasCustomShardingExpr())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`sharding_expr` cannot be set when the `{}` column is defined",
                ProtonConsts::RESERVED_MESSAGE_KEY);

        if (settings->isChanged("one_message_per_row") && !settings->one_message_per_row)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`one_message_per_row` cannot be set to `false` when the `{}` column is defined",
                ProtonConsts::RESERVED_MESSAGE_KEY);

        settings->set("one_message_per_row", true);
    }

    if (!settings->message_key.value.empty())
    {
        validateMessageKey(settings->message_key.value, storage, context);

        if (hasCustomShardingExpr())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "`sharding_expr` and `message_key` cannot be used together");

        /// When message_key is set, each row should be sent as one message, it doesn't make any sense otherwise.
        if (settings->isChanged("one_message_per_row") && !settings->one_message_per_row)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "`one_message_per_row` cannot be set to `false` when `message_key` is set");
        settings->set("one_message_per_row", true);
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

    client = DB::Kafka::ConnectionFactory::instance().getConnection(std::move(conf));

    if (!attach)
        /// Only validate cluster / topic for external stream creation
        validate();
}

void Kafka::startup()
{
    LOG_INFO(logger, "Starting Kafka External Stream");
}

void Kafka::shutdown()
{
    LOG_INFO(logger, "Shutting down Kafka External Stream");

    /// Release all resources here rather than relying on the deconstructor.
    /// Because the `Kafka` instance will not be destroyed immediately when the external stream gets dropped.
    client.reset();

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

std::vector<Int64> Kafka::getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const
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

        return client->getOffsetsForTimestamps(settings->topic.value, partition_timestamps);
    }
}

void Kafka::validateMessageKey(const String & message_key_, IStorage * storage, const ContextPtr & context)
{
    const auto & key = message_key_.c_str();
    Tokens tokens(key, key + message_key_.size(), 0);
    IParser::Pos pos(tokens, 0);
    Expected expected;
    ParserExpression p_id;
    if (!p_id.parse(pos, message_key_ast, expected))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "message_key was not a valid expression, parse failed at {}, expected {}",
            expected.max_parsed_pos,
            fmt::join(expected.variants, ", "));

    if (!pos->isEnd())
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "message_key must be a single expression, got extra characters: {}",
            expected.max_parsed_pos);

    auto syntax_result = TreeRewriter(context).analyze(message_key_ast, storage->getInMemoryMetadata().getColumns().getAllPhysical());
    auto analyzer = ExpressionAnalyzer(message_key_ast, syntax_result, context).getActions(true);
    const auto & block = analyzer->getSampleBlock();
    if (block.columns() != 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key expression must return exactly one column");

    auto type_id = block.getByPosition(0).type->getTypeId();
    if (type_id != TypeIndex::String && type_id != TypeIndex::FixedString)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key must have type of string");
}

/// Validate the topic still exists, specified partitions are still valid etc
void Kafka::validate()
{
    if (client->getPartitionCount(topicName()) < 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Topic has no partitions, topic={}", topicName());
}

std::optional<UInt64> Kafka::totalRows(const Settings & settings_ref) const
{
    /// Only optimize trivial count when one message represents one row.
    if (!settings->one_message_per_row.value)
        return {};

    auto shards_to_query = getShardsToQuery(settings_ref.shards.value, client->getPartitionCount(topicName()));
    LOG_INFO(logger, "Counting number of messages topic={} partitions=[{}]", topicName(), fmt::join(shards_to_query, ","));

    UInt64 rows = 0;
    for (auto shard : shards_to_query)
    {
        auto marks = client->getConsumer(topicName())->queryWatermarkOffsets(shard);
        LOG_INFO(logger, "Watermark offsets topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
        rows += marks.high - marks.low;
    }
    return rows;
}

std::vector<int64_t> Kafka::getLastSNs() const
{
    auto partitions = client->getPartitionCount(topicName());

    std::vector<int64_t> result;
    result.reserve(partitions);

    for (int32_t i = 0; i < partitions; ++i)
    {
        auto offset = client->getWatermarkOffsets(topicName(), i);
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
    /// The consumer can be shared between all the sources in the same pipe, because each source reads from a different partition.
    auto consumer = client->getConsumer(topicName());

    /// User can explicitly consume specific kafka partitions by specifying `shards=` setting
    /// `SELECT * FROM kafka_stream SETTINGS shards=0,3`
    auto shards_to_query = getShardsToQuery(context->getSettingsRef().shards.value, consumer->getPartitionCount());
    assert(!shards_to_query.empty());

    auto streaming = query_info.syntax_analyzer_result->streaming;

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
                auto marks = consumer->queryWatermarkOffsets(shard);
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
                else if (offset == ProtonConsts::LatestSN || offset > marks.high)
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
                settings->consumer_stall_timeout_ms.totalMilliseconds(),
                external_stream_counter,
                logger,
                context));
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
    auto producer = client->getProducer(topicName());

    auto sink = std::make_shared<KafkaSink>(
        *this,
        metadata_snapshot->getSampleBlock(),
        message_key_ast,
        producer,
        external_stream_counter,
        &Poco::Logger::get(fmt::format("{}.{}", getLoggerName(), producer->name())),
        context);

    producer->start();
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
        LOG_WARNING(
            cbLogger(),
            "Error occurred on {}, error={}, reason={}",
            rd_kafka_name(rk),
            rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(err)),
            reason);
    }
}

void Kafka::onThrottle(struct rd_kafka_s * /*rk*/, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * /*opaque*/)
{
    LOG_WARNING(cbLogger(), "Throttled on broker={}, broker_id={}, throttle_time_ms={}", broker_name, broker_id, throttle_time_ms);
}

}

}
