#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/KafkaSink.h>
#include <Storages/ExternalStream/Kafka/KafkaSource.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <filesystem>
#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int INVALID_SETTING_VALUE;
extern const int NO_AVAILABLE_KAFKA_CONSUMER;
}

namespace
{

const String MAX_CONSUMERS_CONFIG_KEY = "external_stream.kafka.max_consumers_per_stream";
const size_t DEFAULT_MAX_CONSUMERS = 50;

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
        "sasl.username",
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

Kafka::ConfPtr createConfFromSettings(const KafkaExternalStreamSettings & settings)
{
    if (settings.brokers.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `brokers` setting for kafka external stream");

    Kafka::ConfPtr conf{rd_kafka_conf_new(), rd_kafka_conf_destroy};
    char errstr[512]{'\0'};

    auto conf_set = [&](const String & name, const String & value) {
        auto err = rd_kafka_conf_set(conf.get(), name.c_str(), value.c_str(), errstr, sizeof(errstr));
        if (err != RD_KAFKA_CONF_OK)
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to set kafka config `{}` with value `{}` error_code={} error_msg={}",
                name,
                value,
                err,
                errstr);
        }
    };

    /// 1. Set default values
    /// -- For Producer
    conf_set("enable.idempotence", "true");
    conf_set("message.timeout.ms", "0" /* infinite */);
    /// -- For Consumer
    /// If the desired offset is out of range, read from the beginning to avoid data lost.
    conf_set("auto.offset.reset", "earliest");

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

        conf_set(key, value);
    }

    /// 3. Handle the speicific settings have higher priority
    conf_set("bootstrap.servers", settings.brokers.value);

    conf_set("security.protocol", settings.security_protocol.value);
    if (settings.usesSASL())
    {
        conf_set("sasl.mechanism", settings.sasl_mechanism.value);
        conf_set("sasl.username", settings.username.value);
        conf_set("sasl.password", settings.password.value);
    }

    if (settings.usesSecureConnection())
    {
        conf_set("enable.ssl.certificate.verification", settings.skip_ssl_cert_check ? "false" : "true");
        if (!settings.ssl_ca_cert_file.value.empty())
            conf_set("ssl.ca.location", settings.ssl_ca_cert_file.value);
    }

    return conf;
}

}

const String Kafka::VIRTUAL_COLUMN_MESSAGE_KEY = "_message_key";

Kafka::ConfPtr Kafka::createRdConf(KafkaExternalStreamSettings settings_)
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
    bool attach,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr context)
    : StorageExternalStreamImpl(storage, std::move(settings_), context)
    , engine_args(engine_args_)
    , data_format(StorageExternalStreamImpl::dataFormat())
    , external_stream_counter(external_stream_counter_)
    , conf(createRdConf(settings->getKafkaSettings()))
    , poll_timeout_ms(settings->poll_waittime_ms.value)
    , max_consumers(context->getConfigRef().getInt(MAX_CONSUMERS_CONFIG_KEY, DEFAULT_MAX_CONSUMERS))
    , logger(&Poco::Logger::get(getLoggerName()))
{
    assert(settings->type.value == StreamTypes::KAFKA || settings->type.value == StreamTypes::REDPANDA);
    assert(external_stream_counter);

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", settings->type.value);

    if (!settings->message_key.value.empty())
    {
        validateMessageKey(settings->message_key.value, storage, context);

        /// When message_key is set, each row should be sent as one message, it doesn't make any sense otherwise.
        if (settings->isChanged("one_message_per_row") && !settings->one_message_per_row)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "`one_message_per_row` cannot be set to `false` when `message_key` is set");
        settings->set("one_message_per_row", true);
    }

    size_t value_size = 8;
    char topic_refresh_interval_ms_value[8]{'\0'}; /// max: 3600000
    rd_kafka_conf_get(conf.get(), "topic.metadata.refresh.interval.ms", topic_refresh_interval_ms_value, &value_size);
    topic_refresh_interval_ms = std::stoi(topic_refresh_interval_ms_value);

    calculateDataFormat(storage);

    cacheVirtualColumnNamesAndTypes();

    rd_kafka_conf_set_log_cb(conf.get(), &Kafka::onLog);
    rd_kafka_conf_set_error_cb(conf.get(), &Kafka::onError);
    rd_kafka_conf_set_stats_cb(conf.get(), &Kafka::onStats);
    rd_kafka_conf_set_throttle_cb(conf.get(), &Kafka::onThrottle);
    rd_kafka_conf_set_dr_msg_cb(conf.get(), &KafkaSink::onMessageDelivery);

    if (!attach)
        /// Only validate cluster / topic for external stream creation
        validate();
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
    virtual_column_names_and_types.push_back(NameAndTypePair(VIRTUAL_COLUMN_MESSAGE_KEY, std::make_shared<DataTypeString>()));
}

std::vector<Int64> Kafka::getOffsets(
    const RdKafka::Consumer & consumer, const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const
{
    assert(seek_to_info);
    seek_to_info->replicateForShards(shards_to_query.size());
    if (!seek_to_info->isTimeBased())
    {
        return seek_to_info->getSeekPoints();
    }
    else
    {
        std::vector<klog::PartitionTimestamp> partition_timestamps;
        partition_timestamps.reserve(shards_to_query.size());
        auto seek_timestamps{seek_to_info->getSeekPoints()};
        assert(shards_to_query.size() == seek_timestamps.size());

        for (auto [shard, timestamp] : std::ranges::views::zip(shards_to_query, seek_timestamps))
            partition_timestamps.emplace_back(shard, timestamp);

        return consumer.getOffsetsForTimestamps(settings->topic.value, partition_timestamps);
    }
}

void Kafka::calculateDataFormat(const IStorage * storage)
{
    if (!data_format.empty())
        return;

    /// If there is only one column and its type is a string type, use RawBLOB. Use JSONEachRow otherwise.
    auto column_names_and_types{storage->getInMemoryMetadata().getColumns().getOrdinary()};
    if (column_names_and_types.size() == 1)
    {
        auto type = column_names_and_types.begin()->type->getTypeId();
        if (type == TypeIndex::String || type == TypeIndex::FixedString)
        {
            data_format = "RawBLOB";
            return;
        }
    }

    data_format = "JSONEachRow";
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
    auto consumer = getConsumer();
    RdKafka::Topic topic{*consumer->getHandle(), topicName()};
    auto parition_count = topic.getPartitionCount();
    if (parition_count < 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Topic has no paritions, topic={}", topicName());
}

namespace
{
std::vector<int32_t> parseShards(const std::string & shards_setting)
{
    std::vector<String> shard_strings;
    boost::split(shard_strings, shards_setting, boost::is_any_of(","));

    std::vector<int32_t> specified_shards;
    specified_shards.reserve(shard_strings.size());
    for (const auto & shard_string : shard_strings)
    {
        try
        {
            specified_shards.push_back(std::stoi(shard_string));
        }
        catch (std::invalid_argument &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard : {}", shard_string);
        }
        catch (std::out_of_range &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Shard {} is too big", shard_string);
        }

        if (specified_shards.back() < 0)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard: {}", shard_string);
    }

    return specified_shards;
}

std::vector<Int32> getShardsToQuery(const String & shards_exp, Int32 parition_count)
{
    std::vector<Int32> ret;
    if (!shards_exp.empty())
    {
        ret = parseShards(shards_exp);
        /// Make sure they are valid.
        for (auto shard : ret)
        {
            if (shard >= parition_count)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid topic partition {}, the topic has only {} partitions",
                    shard,
                    parition_count);
        }
    }
    else
    {
        /// Query all available shards / partitions
        ret.reserve(parition_count);
        for (int32_t i = 0; i < parition_count; ++i)
            ret.push_back(i);
    }

    return ret;
}
}

std::optional<UInt64> Kafka::totalRows(const Settings & settings_ref)
{
    auto consumer = getConsumer();
    auto topic = RdKafka::Topic(*consumer->getHandle(), topicName());
    auto shards_to_query = getShardsToQuery(settings_ref.shards.value, topic.getPartitionCount());
    LOG_INFO(logger, "Counting number of messages topic={} partitions=[{}]", topicName(), fmt::join(shards_to_query, ","));

    UInt64 rows = 0;
    for (auto shard : shards_to_query)
    {
        auto marks = topic.queryWatermarks(shard);
        LOG_INFO(logger, "Watermarks topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
        rows += marks.high - marks.low;
    }
    return rows;
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
    auto consumer = getConsumer();
    /// The topic_ptr can be shared between all the sources in the same pipe, because each source reads from a different partition.
    auto topic_ptr = std::make_shared<RdKafka::Topic>(*consumer->getHandle(), topicName());

    /// User can explicitly consume specific kafka partitions by specifying `shards=` setting
    /// `SELECT * FROM kafka_stream SETTINGS shards=0,3`
    auto shards_to_query = getShardsToQuery(context->getSettingsRef().shards.value, topic_ptr->getPartitionCount());
    assert(!shards_to_query.empty());

    auto streaming = query_info.syntax_analyzer_result->streaming;

    LOG_INFO(logger, "Reading topic={} partitions=[{}] streaming={}", topicName(), fmt::join(shards_to_query, ","), streaming);

    Pipes pipes;
    pipes.reserve(shards_to_query.size());

    {
        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_TIME});

        auto seek_to_info = query_info.seek_to_info;
        /// seek_to defaults to 'latest' for streaming. In non-streaming case, 'earliest' is preferred.
        if (!streaming && seek_to_info->getSeekTo().empty())
            seek_to_info = std::make_shared<SeekToInfo>("earliest");

        auto offsets = getOffsets(*consumer, seek_to_info, shards_to_query);
        assert(offsets.size() == shards_to_query.size());

        for (auto [shard, offset] : std::ranges::views::zip(shards_to_query, offsets))
        {
            std::optional<Int64> high_watermark = std::nullopt;
            if (!streaming)
            {
                auto marks = topic_ptr->queryWatermarks(shard);
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
                else if (offset == nlog::LATEST_SN || offset > marks.high)
                    offset = marks.high;
            }
            pipes.emplace_back(std::make_shared<KafkaSource>(
                *this,
                header,
                storage_snapshot,
                consumer,
                topic_ptr,
                shard,
                offset,
                high_watermark,
                max_block_size,
                external_stream_counter,
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

std::shared_ptr<RdKafka::Consumer> Kafka::getConsumer()
{
    std::lock_guard<std::mutex> lock{consumer_mutex};

    auto consumer_ref = std::find_if(consumers.begin(), consumers.end(), [](const auto & consumer) { return consumer.expired(); });
    if (consumer_ref == consumers.end() && consumers.size() >= max_consumers)
        throw Exception(
            ErrorCodes::NO_AVAILABLE_KAFKA_CONSUMER,
            "Reached consumers limit {}. Existing queries need to be stopped before running other queries. Or update {} to a bigger number "
            "in the config file",
            max_consumers,
            MAX_CONSUMERS_CONFIG_KEY);

    auto new_consumer = std::make_shared<RdKafka::Consumer>(*conf, poll_timeout_ms, getLoggerName());
    std::weak_ptr<RdKafka::Consumer> ref = new_consumer;

    if (consumer_ref != consumers.end())
        consumer_ref->swap(ref);
    else
        consumers.push_back(ref);

    return new_consumer;
}

std::shared_ptr<RdKafka::Producer> Kafka::getProducer()
{
    if (producer)
        return producer;

    std::lock_guard<std::mutex> lock{producer_mutex};
    /// Check again in case of losing the race
    if (producer)
        return producer;

    auto producer_ptr = std::make_shared<RdKafka::Producer>(*conf, settings->poll_waittime_ms.value, getLoggerName());
    producer.swap(producer_ptr);

    return producer;
}

std::shared_ptr<RdKafka::Topic> Kafka::getProducerTopic()
{
    if (producer_topic)
        return producer_topic;

    std::scoped_lock lock(producer_mutex);
    /// Check again in case of losing the race
    if (producer_topic)
        return producer_topic;

    auto topic_ptr = std::make_shared<RdKafka::Topic>(*getProducer()->getHandle(), topicName());
    producer_topic.swap(topic_ptr);

    return producer_topic;
}

SinkToStoragePtr Kafka::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    /// always validate before actual use
    validate();
    return std::make_shared<KafkaSink>(*this, metadata_snapshot->getSampleBlock(), message_key_ast, external_stream_counter, context);
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
        LOG_ERROR(cbLogger(), "{}|{} buf={}", rd_kafka_name(rk), fac, buf);
    else if (level == 4)
        LOG_WARNING(cbLogger(), "{}|{} buf={}", rd_kafka_name(rk), fac, buf);
    else
        LOG_INFO(cbLogger(), "{}|{} buf={}", rd_kafka_name(rk), fac, buf);
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
