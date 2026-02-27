#include <Storages/ExternalStream/NATSJetstream/NATSJetstream.h>

#if USE_NATSIO

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/NATSJetstream/NATSJetstreamSink.h>
#include <Storages/ExternalStream/NATSJetstream/NATSJetstreamSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>

#include <Cluster/Common/Constants.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONNECT_SERVER;
extern const int ILLEGAL_COLUMN;
extern const int INVALID_SETTING_VALUE;
extern const int UNSUPPORTED;
}

namespace
{


void validateMessageKeyColumnType(const DataTypePtr & type)
{
    if (type->isNullable())
    {
        validateMessageKeyColumnType(static_cast<const DataTypeNullable &>(*type).getNestedType());
        return;
    }

    auto type_id = type->getTypeId();
    if (type_id != TypeIndex::String && type_id != TypeIndex::FixedString)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "`_tp_message_key` column for NATS JetStream only supports String type (NATS subjects are strings), got {}",
            type->getName());
}


void validateMessageHeadersColumnType(const DataTypePtr & type)
{
    if (!WhichDataType{type}.isMap())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_headers` column must have type of map(string, string)");

    const auto & map_type = dynamic_cast<const DataTypeMap &>(*type);
    if (!WhichDataType{map_type.getKeyType()}.isStringOrFixedString() || !WhichDataType{map_type.getValueType()}.isStringOrFixedString())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_headers` column must have type of map(string, string)");
}

} /// anonymous namespace

namespace ExternalStream
{

namespace
{


ExternalStreamSettingsPtr validateNATSSettings(ExternalStreamSettingsPtr && settings)
{
    assert(settings->type.value == StreamTypes::NATS_JETSTREAM);

    if (settings->url.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "NATS JetStream: 'url' cannot be empty");

    if (settings->subject.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "NATS JetStream: 'subject' cannot be empty");

    if (settings->stream_name.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "NATS JetStream: 'stream_name' cannot be empty");

    const auto & ack = settings->ack_policy.value;
    if (ack != "none" && ack != "all" && ack != "explicit")
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "NATS JetStream: 'ack_policy' must be 'none', 'all', or 'explicit', got '{}'", ack);

    const auto & deliver = settings->deliver_policy.value;
    if (deliver != "all" && deliver != "last" && deliver != "new"
        && deliver != "by_start_sequence" && deliver != "by_start_time")
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "NATS JetStream: 'deliver_policy' must be 'all', 'last', 'new', "
            "'by_start_sequence', or 'by_start_time', got '{}'", deliver);

    /// Only one auth mechanism at a time
    int auth_count = 0;
    if (!settings->nats_username.value.empty() && !settings->nats_password.value.empty())
        ++auth_count;
    if (!settings->nats_token.value.empty())
        ++auth_count;
    if (!settings->nats_creds_file.value.empty())
        ++auth_count;
    if (!settings->nats_nkey_seed.value.empty())
        ++auth_count;

    if (auth_count > 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "NATS JetStream: only one authentication method can be used at a time "
            "(username/password, token, credentials file, or NKey)");


    if ((!settings->nats_cert_file.value.empty()) != (!settings->nats_key_file.value.empty()))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "NATS JetStream: 'nats_cert_file' and 'nats_key_file' must be used together for mTLS");

    return std::move(settings);
}

} /// anonymous namespace

NATSJetstream::NATSJetstream(
    StorageID storage_id,
    StorageInMemoryMetadata metadata,
    ExternalStreamSettingsPtr settings_,
    bool attach,
    ExternalStreamCounterPtr counter,
    ContextPtr context)
    : StorageExternalStreamImpl(
          std::move(storage_id),
          std::move(metadata),
          validateNATSSettings(std::move(settings_)),
          context)
    , external_stream_counter(std::move(counter))
{
    assert(external_stream_counter);

    validateSettings(attach);
    cacheVirtualColumnNamesAndTypes();

    if (!attach)
    {
        /// Check stream exists on CREATE, skip on ATTACH
        connect();

        jsStreamInfo * stream_info = nullptr;
        jsOptions js_opts;
        jsOptions_Init(&js_opts);

        natsStatus status = js_GetStreamInfo(&stream_info, js_context, getStreamName().c_str(), &js_opts, nullptr);
        if (status != NATS_OK)
        {
            disconnect();
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_SERVER,
                "Failed to get JetStream stream info for '{}' on {}: {} (ensure the stream exists)",
                getStreamName(),
                getUrl(),
                natsStatus_GetText(status));
        }

        UInt64 num_subjects = 0;
        if (stream_info && stream_info->State.Subjects)
            num_subjects = stream_info->State.Subjects->Count;

        LOG_INFO(
            logger,
            "Created NATS JetStream external stream: url={} stream={} subject='{}' subjects_in_stream={} "
            "ack_policy={} deliver_policy={} one_message_per_row={} stall_timeout_ms={}",
            getUrl(),
            getStreamName(),
            getSubject(),
            num_subjects,
            getAckPolicy(),
            settings->deliver_policy.value,
            produceOneMessagePerRow(),
            stallTimeoutMs());

        jsStreamInfo_Destroy(stream_info);
        disconnect();
    }
}

NATSJetstream::~NATSJetstream()
{
    try
    {
        disconnect();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error during NATS JetStream cleanup");
    }
}

void NATSJetstream::validateSettings(bool attach)
{
    const auto & columns = getInMemoryMetadataPtr()->getColumns();
    const bool has_event_time = columns.has(ProtonConsts::RESERVED_EVENT_TIME);
    const bool has_message_key = columns.has(ProtonConsts::RESERVED_MESSAGE_KEY);
    const bool has_message_headers = columns.has(ProtonConsts::RESERVED_MESSAGE_HEADERS);

    /// _tp_time is a virtual column for NATS — prevent defining as physical
    if (has_event_time)
    {
        if (attach)
        {
            LOG_WARNING(
                logger,
                "Column `{}` is a reserved virtual column for NATS JetStream external streams and is no longer supported as a physical column. "
                "It will be ignored in payload parsing and treated as transport metadata.",
                ProtonConsts::RESERVED_EVENT_TIME);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Column `{}` is a reserved virtual column for NATS JetStream external streams and cannot be defined as a physical column",
                ProtonConsts::RESERVED_EVENT_TIME);
        }
    }

    /// Validate _tp_message_key column type
    if (has_message_key)
        validateMessageKeyColumnType(columns.getColumn({GetColumnsOptions::Kind::All}, ProtonConsts::RESERVED_MESSAGE_KEY).type);

    /// Validate _tp_message_headers column type
    if (has_message_headers)
        validateMessageHeadersColumnType(columns.getColumn({GetColumnsOptions::Kind::All}, ProtonConsts::RESERVED_MESSAGE_HEADERS).type);

    /// When _tp_message_key, _tp_time, or _tp_message_headers is defined, one_message_per_row must be true
    if (has_event_time || has_message_key || has_message_headers)
    {
        if (settings->isChanged("one_message_per_row") && !settings->one_message_per_row)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "`one_message_per_row` cannot be set to `false` when the `{}` / `{}` / `{}` column is defined",
                ProtonConsts::RESERVED_EVENT_TIME,
                ProtonConsts::RESERVED_MESSAGE_KEY,
                ProtonConsts::RESERVED_MESSAGE_HEADERS);

        settings->set("one_message_per_row", true);
    }
}

natsOptions * NATSJetstream::createConnectionOptions()
{
    natsOptions * opts = nullptr;
    natsOptions_Create(&opts);

    natsOptions_SetURL(opts, getUrl().c_str());

    /// Reconnection settings
    natsOptions_SetMaxReconnect(opts, static_cast<int>(settings->max_reconnects.value));
    natsOptions_SetReconnectWait(opts, static_cast<int64_t>(settings->reconnect_wait_ms.value));
    natsOptions_SetReconnectBufSize(opts, 8 * 1024 * 1024); /// 8MB reconnect buffer
    natsOptions_SetRetryOnFailedConnect(opts, true, nullptr, nullptr);

    /// Only one mechanism at a time
    const auto & username = settings->nats_username.value;
    const auto & password = settings->nats_password.value;
    const auto & token    = settings->nats_token.value;
    const auto & creds    = settings->nats_creds_file.value;
    const auto & nkey     = settings->nats_nkey_seed.value;

    if (!username.empty() && !password.empty())
    {
        natsOptions_SetUserInfo(opts, username.c_str(), password.c_str());
        LOG_INFO(logger, "NATS: using username/password authentication");
    }
    else if (!token.empty())
    {
        natsOptions_SetToken(opts, token.c_str());
        LOG_INFO(logger, "NATS: using token authentication");
    }
    else if (!creds.empty())
    {
        natsOptions_SetUserCredentialsFromFiles(opts, creds.c_str(), nullptr);
        LOG_INFO(logger, "NATS: using credentials file authentication");
    }
    else if (!nkey.empty())
    {
        natsOptions_SetNKey(opts, nkey.c_str(), nullptr, nullptr);
        LOG_INFO(logger, "NATS: using NKey authentication");
    }

    /// TLS configuration
    bool use_tls = settings->nats_tls.value
        || getUrl().starts_with("tls://")
        || getUrl().starts_with("nats+tls://");

    if (use_tls)
    {
        natsOptions_SetSecure(opts, true);

        const auto & ca_file   = settings->nats_ca_file.value;
        const auto & cert_file = settings->nats_cert_file.value;
        const auto & key_file  = settings->nats_key_file.value;

        if (!ca_file.empty())
        {
            natsOptions_SetCATrustedCertificates(opts, ca_file.c_str());
            LOG_INFO(logger, "NATS: TLS CA cert loaded from '{}'", ca_file);
        }

        if (!cert_file.empty() && !key_file.empty())
        {
            natsOptions_SetCertificatesChain(opts, cert_file.c_str(), key_file.c_str());
            LOG_INFO(logger, "NATS: mTLS configured with client cert");
        }
    }

    return opts;
}

void NATSJetstream::connect()
{
    std::lock_guard lock(connection_mutex);

    if (connection != nullptr)
        return;

    natsOptions * opts = createConnectionOptions();
    natsStatus status = natsConnection_Connect(&connection, opts);
    natsOptions_Destroy(opts);

    if (status != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to connect to NATS server at {}: {}",
            getUrl(),
            natsStatus_GetText(status));


    jsOptions js_opts;
    jsOptions_Init(&js_opts);

    status = natsConnection_JetStream(&js_context, connection, &js_opts);
    if (status != NATS_OK)
    {
        natsConnection_Destroy(connection);
        connection = nullptr;
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to create JetStream context on {}: {}",
            getUrl(),
            natsStatus_GetText(status));
    }

    LOG_INFO(logger, "Connected to NATS JetStream at {}", getUrl());
}

void NATSJetstream::disconnect()
{
    std::lock_guard lock(connection_mutex);

    if (js_context)
    {
        jsCtx_Destroy(js_context);
        js_context = nullptr;
    }

    if (connection)
    {
        natsConnection_Drain(connection);
        natsConnection_Destroy(connection);
        connection = nullptr;
    }
}

jsCtx * NATSJetstream::getJetStreamContext()
{
    /// NOTE: The returned pointer is valid only while this storage object is alive and not
    /// disconnected. Callers (e.g. NATSJetstreamSink) must not hold the pointer across
    /// a potential disconnect/reconnect cycle.
    std::lock_guard lock(connection_mutex);
    return js_context;
}

natsConnection * NATSJetstream::getConnection()
{
    std::lock_guard lock(connection_mutex);
    return connection;
}

natsSubscription * NATSJetstream::createPullSubscription(
    const String & consumer_name_override,
    const ContextPtr & /*context*/)
{
    jsSubOptions sub_opts;
    jsSubOptions_Init(&sub_opts);
    sub_opts.Stream = getStreamName().c_str();

    /// Configure consumer
    jsConsumerConfig consumer_cfg;
    jsConsumerConfig_Init(&consumer_cfg);

    consumer_cfg.Name = consumer_name_override.c_str();
    if (isDurable())
        consumer_cfg.Durable = consumer_name_override.c_str();

    /// Ack policy
    const auto & ack = settings->ack_policy.value;
    if (ack == "none")
        consumer_cfg.AckPolicy = js_AckNone;
    else if (ack == "all")
        consumer_cfg.AckPolicy = js_AckAll;
    else
        consumer_cfg.AckPolicy = js_AckExplicit;

    /// Deliver policy
    const auto & deliver = settings->deliver_policy.value;
    if (deliver == "all")
        consumer_cfg.DeliverPolicy = js_DeliverAll;
    else if (deliver == "last")
        consumer_cfg.DeliverPolicy = js_DeliverLast;
    else if (deliver == "new")
        consumer_cfg.DeliverPolicy = js_DeliverNew;
    else if (deliver == "by_start_sequence")
    {
        consumer_cfg.DeliverPolicy = js_DeliverByStartSequence;
        consumer_cfg.OptStartSeq = settings->start_sequence.value;
    }
    else if (deliver == "by_start_time")
    {
        consumer_cfg.DeliverPolicy = js_DeliverByStartTime;

        /// start_time is expected as a Unix timestamp in nanoseconds (int64 in string form).
        /// Example: "1700000000000000000" for 2023-11-14T22:13:20Z
        const auto & time_str = settings->start_time.value;
        if (!time_str.empty())
        {
            try
            {
                consumer_cfg.OptStartTime = std::stoll(time_str);
            }
            catch (const std::exception &)
            {
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "NATS JetStream: 'start_time' must be a Unix timestamp in nanoseconds, got '{}'",
                    time_str);
            }
        }
    }

    consumer_cfg.MaxAckPending = static_cast<int64_t>(settings->max_ack_pending.value);

    sub_opts.Config = consumer_cfg;

    natsSubscription * subscription = nullptr;
    natsStatus status;
    {
        /// Hold the lock while accessing js_context to prevent use-after-free
        /// if disconnect() is called concurrently on another thread.
        std::lock_guard lock(connection_mutex);
        status = js_PullSubscribe(
            &subscription,
            js_context,
            getSubject().c_str(),
            consumer_name_override.c_str(),
            nullptr,
            &sub_opts,
            nullptr);
    }

    if (status != NATS_OK)
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to create JetStream pull subscription: subject='{}' stream='{}' consumer='{}': {}",
            getSubject(),
            getStreamName(),
            consumer_name_override,
            natsStatus_GetText(status));

    return subscription;
}

void NATSJetstream::startup()
{
    StorageExternalStreamImpl::startup();

    connect();
    LOG_INFO(logger, "NATS JetStream external stream started: stream={} subject='{}'", getStreamName(), getSubject());
}

void NATSJetstream::shutdown(bool /*dropping*/)
{
    LOG_INFO(logger, "NATS JetStream external stream shutting down: stream={}", getStreamName());
    disconnect();
}

NamesAndTypesList NATSJetstream::getVirtuals() const
{
    return virtual_column_names_and_types;
}

void NATSJetstream::cacheVirtualColumnNamesAndTypes()
{
    /// Standard virtual columns plus NATS-specific ones
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_KEY, std::make_shared<DataTypeString>()));

    DataTypes header_types{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()};
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_MESSAGE_HEADERS, std::make_shared<DataTypeMap>(header_types)));

    /// NATS-specific virtual columns
    virtual_column_names_and_types.push_back(
        NameAndTypePair("_nats_subject", std::make_shared<DataTypeString>()));
    virtual_column_names_and_types.push_back(
        NameAndTypePair("_nats_timestamp", std::make_shared<DataTypeDateTime64>(3, "UTC")));
}

std::optional<String> NATSJetstream::preferredColumn() const
{
    return ProtonConsts::RESERVED_EVENT_SEQUENCE_ID;
}

ExternalStreamSettingsPtr NATSJetstream::validateAndMoveSettings(ExternalStreamSettingsPtr && settings_)
{
    return validateNATSSettings(std::move(settings_));
}

Pipe NATSJetstream::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr query_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    Block header;
    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
        header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_EVENT_SEQUENCE_ID});

    auto streaming = query_info.isStreaming();
    auto format_settings = getFormatSettings(query_context);

    /// Generate consumer name: user-specified or auto from query ID
    String consumer_name = getConsumerName();
    if (consumer_name.empty())
        consumer_name = fmt::format("proton-{}", query_context->getCurrentQueryId());

    auto * subscription = createPullSubscription(consumer_name, query_context);

    auto this_ptr = std::static_pointer_cast<NATSJetstream>(shared_from_this());

    auto source = std::make_shared<NATSJetstreamSource>(
        header,
        storage_snapshot,
        streaming,
        dataFormat(),
        format_settings,
        subscription,
        this_ptr,
        max_block_size,
        stallTimeoutMs(),
        external_stream_counter,
        logger,
        query_context);

    Pipes pipes;
    pipes.emplace_back(std::move(source));

    LOG_INFO(
        logger,
        "Starting reading {} streams from NATS JetStream: subject='{}' stream={} consumer={} streaming={} seek_to={}",
        pipes.size(),
        getSubject(),
        getStreamName(),
        consumer_name,
        streaming,
        query_info.seek_to_info->getSeekTo());

    return Pipe::unitePipes(std::move(pipes));
}

SinkToStoragePtr NATSJetstream::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr query_context)
{
    return std::make_shared<NATSJetstreamSink>(
        *this,
        metadata_snapshot->getSampleBlock(),
        external_stream_counter,
        getLogger(fmt::format("{}.sink", getLoggerName())),
        query_context);
}

} /// namespace ExternalStream

} /// namespace DB

#endif
