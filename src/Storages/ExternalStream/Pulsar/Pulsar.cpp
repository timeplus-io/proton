#include <Storages/ExternalStream/Pulsar/Pulsar.h>

#if USE_PULSAR

#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromFile.h>
#    include <Interpreters/TreeRewriter.h>
#    include <Record/Record.h>
#    include <Storages/ExternalStream/ExternalStreamTypes.h>
#    include <Storages/ExternalStream/Pulsar/Logger.h>
#    include <Storages/ExternalStream/Pulsar/PulsarSource.h>
#    include <Storages/ExternalStream/parseShards.h>
#    include <Storages/SelectQueryInfo.h>
#    include <Common/ProtonCommon.h>

#    include <pulsar/Client.h>
#    include <pulsar/Producer.h>
#    include <pulsar/ProducerConfiguration.h>
#    include <pulsar/Result.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONNECT_SERVER;
extern const int INVALID_SETTING_VALUE;
}

namespace ExternalStream
{

namespace
{

ExternalStreamSettingsPtr validateSettings(ExternalStreamSettingsPtr && settings)
{
    assert(settings->type.value == StreamTypes::PULSAR);

    if (settings->service_url.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "service_url cannot be empty");

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "topic cannot be empty");

    /// Make sure that client_cert and client_key are both empty or both not empty.
    if ((settings->client_cert.value.empty() && !settings->client_key.value.empty())
        || (!settings->client_cert.value.empty() && settings->client_key.value.empty()))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "client_cert and client_key must be used together");

    /// Only one authentication can be used.
    if (!settings->client_cert.value.empty() && !settings->client_key.value.empty() && !settings->jwt.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "client_cert and client_key cannot be used with jwt at the same time");

    return std::move(settings);
}

}

Pulsar::Pulsar(IStorage * storage, ExternalStreamSettingsPtr settings_, bool attach, ExternalStreamCounterPtr counter, ContextPtr context)
    : StorageExternalStreamImpl(storage, validateSettings(std::move(settings_)), context)
    , client(serviceUrl(), createClientConfig())
    , external_stream_counter(counter)
{
    if (!attach)
    {
        std::vector<String> partitions;
        auto res = client.getPartitionsForTopic(settings->topic.value, partitions);
        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Failed to get paritions of topic {} on {}, error={}",
                settings->topic.value,
                serviceUrl(),
                pulsar::strResult(res));

        LOG_INFO(
            logger,
            "Created Pulsar external stream on topic {} with {} partitions: [{}]",
            settings->topic.value,
            partitions.size(),
            fmt::join(partitions, ", "));
    }

    cacheVirtualColumnNamesAndTypes();
}

pulsar::ClientConfiguration Pulsar::createClientConfig()
{
    pulsar::ClientConfiguration config;

    config.setUseTls(serviceUrl().starts_with("pulsar+ssl://"));

    if (settings->skip_server_cert_check)
        config.setTlsAllowInsecureConnection(true);

    if (settings->validate_hostname)
        config.setValidateHostName(true);

    const auto & ca_cert = settings->ca_cert.value;
    const auto & client_cert = settings->client_cert.value;
    const auto & client_key = settings->client_key.value;

    if (!ca_cert.empty() || !client_cert.empty() || !client_key.empty())
    {
        /// When certificates are used, make sure TLS is enabled.
        config.setUseTls(true);

        createTempDirIfNotExists();

        if (!ca_cert.empty())
        {
            auto ca_cert_file = tmpdir / "ca_cert.pem";
            WriteBufferFromFile(ca_cert_file).write(ca_cert.data(), ca_cert.size());

            config.setTlsTrustCertsFilePath(ca_cert_file);
            /// When ca_cert is provided, it should only accept valid server certs.
            config.setTlsAllowInsecureConnection(false);
        }


        if (!client_cert.empty())
        {
            assert(!client_key.empty());

            LOG_INFO(logger, "Creating client with mTLS authentication");

            auto client_cert_file = tmpdir / "client_cert.pem";
            WriteBufferFromFile(client_cert_file).write(client_cert.data(), client_cert.size());

            auto client_key_file = tmpdir / "client_key.pem";
            WriteBufferFromFile(client_key_file).write(client_key.data(), client_key.size());

            config.setAuth(pulsar::AuthTls::create(client_cert_file, client_key_file));
        }
    }

    if (const auto & jwt = settings->jwt.value; !jwt.empty())
    {
        LOG_INFO(logger, "Creating client with JWT authentication");
        config.setAuth(pulsar::AuthToken::createWithToken(jwt));
    }

    /// The pulsar library has a wired design. Even though ClientConfiguration provides the setLogger
    /// function, but in fact, it can only set the logger once. Any subsequent calls to set the logger factory will have no effect (and it will destroy the passed in LoggerFactory pointer immediately)!
    /// To avoid such meaningless memory allocation, a flag is used to make sure it only call setLogger once.
    if (!pulsar_logger_set.test_and_set())
        config.setLogger(new PulsarLoggerFactory(&Poco::Logger::get("PulsarExternalStream")));

    return config;
}

void Pulsar::startup()
{
    LOG_INFO(logger, "Started");
}

void Pulsar::shutdown()
{
    LOG_INFO(logger, "Shutting down");
    client.close();
}

NamesAndTypesList Pulsar::getVirtuals() const
{
    NamesAndTypesList virtuals;
    for (const auto & col : virtual_columns)
        virtuals.push_back({col.first, col.second.first});
    return virtuals;
}

void Pulsar::cacheVirtualColumnNamesAndTypes()
{
    virtual_columns[ProtonConsts::RESERVED_APPEND_TIME] = {
        std::make_shared<DataTypeDateTime64>(3, "UTC"), [](const pulsar::Message & msg) { return Decimal64(msg.getPublishTimestamp()); }};

    virtual_columns[ProtonConsts::RESERVED_EVENT_TIME] = {std::make_shared<DataTypeDateTime64>(3, "UTC"), [](const pulsar::Message & msg) {
                                                              auto ts = msg.getEventTimestamp();
                                                              if (ts != 0u)
                                                                  return Decimal64(ts);
                                                              else
                                                                  return Decimal64(msg.getPublishTimestamp());
                                                          }};

    DataTypePtr dptr = std::make_shared<DataTypeDateTime64>(3, "UTC");
    virtual_columns[ProtonConsts::RESERVED_PROCESS_TIME]
        = {std::make_shared<DataTypeDateTime64>(3, "UTC"), [](const pulsar::Message &) { return Decimal64(UTCMilliseconds::now()); }};

    virtual_columns[ProtonConsts::RESERVED_SHARD]
        = {std::make_shared<DataTypeInt32>(), [](const pulsar::Message & msg) -> Int32 { return msg.getMessageId().partition(); }};

    virtual_columns[ProtonConsts::RESERVED_EVENT_SEQUENCE_ID]
        = {std::make_shared<DataTypeInt64>(), [](const pulsar::Message & msg) -> Int64 { return msg.getIndex(); }};

    virtual_columns[ProtonConsts::RESERVED_MESSAGE_KEY]
        = {std::make_shared<DataTypeString>(), [](const pulsar::Message & msg) -> Field { return msg.getPartitionKey(); }};

    virtual_columns["_pulsar_message_id"]
        = {std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()), [](const pulsar::Message & msg) -> Array {
               const auto & id = msg.getMessageId();
               return Array({id.ledgerId(), id.entryId(), id.partition(), id.batchIndex()});
           }};
}

std::vector<String> Pulsar::getPartitions(const ContextPtr & query_context)
{
    std::vector<String> partitions;
    if (auto res = client.getPartitionsForTopic(settings->topic.value, partitions); res != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to get paritions of topic {} on {}: {}",
            settings->topic.value,
            serviceUrl(),
            pulsar::strResult(res));

    auto shards_setting = query_context->getSettingsRef().shards.value;

    if (shards_setting.empty())
        return partitions;

    auto shards = parseShards(shards_setting);
    std::vector<String> result;
    result.reserve(shards.size());

    /// Pulsar names partitions with the format of "<topic-name>-partition-<N>" (N is a number).
    /// But for simplicity, we allow users to just use the numbers for `shards`.
    String partition_prefix = settings->topic.value + "-partition-";
    for (const auto & shard : shards)
    {
        auto suffix = fmt::format("{}{}", partition_prefix, shard);
        if (auto it
            = std::find_if(partitions.begin(), partitions.end(), [&suffix](const auto & partition) { return partition.ends_with(suffix); });
            it != partitions.end())
            result.push_back(*it);
        else
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard {}, available paritions: {}", shard, fmt::join(partitions, ", "));
    }

    return result;
}

Pipe Pulsar::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    /// TODO check IProcessor::Status::ExpandPipeline

    /// User can explicitly consume specific pulsar partitions by specifying `shards=` setting
    /// `SELECT * FROM pulsar_stream SETTINGS shards=0,3`
    auto partitions = getPartitions(query_context);

    auto header = storage_snapshot->getSampleBlockForColumns(column_names);
    auto virtual_header = calculateVirtualHeader(header, storage_snapshot->metadata->getSampleBlockNonMaterialized());

    auto streaming = query_info.syntax_analyzer_result->streaming;
    auto seek_to_info = query_info.seek_to_info;
    /// seek_to defaults to 'latest' for streaming. In non-streaming case, 'earliest' is preferred.
    if (!streaming && seek_to_info->getSeekTo().empty())
        seek_to_info = std::make_shared<SeekToInfo>("earliest");

    seek_to_info->replicateForShards(static_cast<UInt32>(partitions.size()));

    const auto & seek_points = seek_to_info->getSeekPoints();
    if (!seek_to_info->isTimeBased())
        for (auto p : seek_points)
            if (p != nlog::LATEST_SN && p != nlog::EARLIEST_SN)
                throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Number based seek_to is not supported in Pulsar external stream");

    Pipes pipes;
    pipes.reserve(partitions.size());

    for (size_t i = 0; const auto & pa : partitions)
    {
        auto reader = createReader(query_context, pa);
        if (seek_to_info->isTimeBased())
            reader.seek(seek_points[i++]);
        else
            reader.seek(seek_points[i++] == nlog::LATEST_SN ? pulsar::MessageId::latest() : pulsar::MessageId::earliest());

        auto read_buffer = std::make_unique<ReadBufferFromMemory>("", 0);
        auto format_executor = createInputFormatExecutor(storage_snapshot, header, *read_buffer, max_block_size, query_context);

        pipes.emplace_back(std::make_shared<PulsarSource>(
            header,
            virtual_header,
            streaming,
            std::move(read_buffer),
            std::move(format_executor),
            std::move(reader),
            external_stream_counter,
            logger,
            query_context));
    }

    LOG_INFO(
        logger,
        "Starting reading {} streams by seeking to {} dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = query_context->getSettingsRef().min_threads.value;
    if (min_threads > partitions.size())
        pipe.resize(min_threads);

    return pipe;
}


SinkToStoragePtr Pulsar::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    const auto & header = metadata_snapshot->getSampleBlock();

    auto max_rows_per_message = query_context->getSettingsRef().max_insert_block_size.value;
    if (settings->one_message_per_row || data_format == "RawBLOB" || header.has(ProtonConsts::RESERVED_MESSAGE_KEY))
        max_rows_per_message = 1;

    return std::make_shared<PulsarSink>(
        header,
        createProducer(query_context),
        data_format,
        getFormatSettings(query_context),
        max_rows_per_message,
        query_context->getSettingsRef().max_insert_block_bytes.value,
        external_stream_counter,
        logger,
        query_context);
}

pulsar::Reader Pulsar::createReader(const ContextPtr & context, const String & partition)
{
    pulsar::ReaderConfiguration config;
    auto name = fmt::format("tp-{}-{}", getLoggerName(), context->getCurrentQueryId());
    config.setReaderName(name);

    pulsar::Reader reader;
    if (auto res = client.createReader(partition, pulsar::MessageId::earliest(), config, reader); res != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to create reader for partition {} on {}: {}",
            partition,
            serviceUrl(),
            pulsar::strResult(res));

    return reader;
}

pulsar::Producer Pulsar::createProducer(const ContextPtr & context)
{
    const auto & query_settings = context->getSettingsRef();

    pulsar::ProducerConfiguration config;
    /// Producer name must be unique. Otherwise, it will return ProducerBusy error (because the producer already exists).
    auto name = fmt::format("tp-{}-{}", getLoggerName(), context->getCurrentQueryId());
    config.setProducerName(name);

    /// Batching
    config.setBatchingEnabled(true);
    config.setBatchingMaxMessages(static_cast<UInt32>(query_settings.output_batch_max_messages));
    config.setBatchingMaxPublishDelayMs(query_settings.output_batch_max_delay_ms);
    config.setBatchingMaxAllowedSizeInBytes(query_settings.output_batch_max_size_bytes);

    /// Queueing
    config.setBlockIfQueueFull(true);
    config.setMaxPendingMessages(static_cast<int>(query_settings.pulsar_max_pending_messages.value));

    pulsar::Producer producer;
    if (auto res = client.createProducer(getTopic(), config, producer); res != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to create producer for topic {} on {}: {}",
            getTopic(),
            serviceUrl(),
            pulsar::strResult(res));

    return producer;
}

VirtualHeader Pulsar::calculateVirtualHeader(const Block & header, const Block & non_virtual_header)
{
    VirtualHeader virtual_header;

    for (size_t pos = 0; pos < header.columns(); pos++)
    {
        const auto & column = header.getByPosition(pos);
        /// The _tp_message_key column always maps to the pulsar message/partition key.
        if (column.name == ProtonConsts::RESERVED_MESSAGE_KEY)
        {
            bool inside_nullable = column.type->getTypeId() == TypeIndex::Nullable;

            if (!inside_nullable)
                virtual_header[pos] = {
                    virtual_columns[ProtonConsts::RESERVED_MESSAGE_KEY].first,
                    virtual_columns[ProtonConsts::RESERVED_MESSAGE_KEY].second,
                };
            else
                virtual_header[pos] = {column.type, [](const pulsar::Message & msg) -> Field {
                                           if (!msg.hasPartitionKey())
                                               return Null{};

                                           return msg.getPartitionKey();
                                       }};
        }
        /// If a virtual column is explicitely defined as a physical column in the stream definition, we should honor it,
        /// just as the virutal columns document says, and users are not recommended to do this (and they still can).
        else if (std::any_of(non_virtual_header.begin(), non_virtual_header.end(), [&column](auto & non_virtual_column) {
                     return non_virtual_column.name == column.name;
                 }))
        {
            continue;
        }
        else if (virtual_columns.contains(column.name))
        {
            virtual_header[pos] = {
                virtual_columns[column.name].first,
                virtual_columns[column.name].second,
            };
        }
    }

    return virtual_header;
}

}

}

#endif
