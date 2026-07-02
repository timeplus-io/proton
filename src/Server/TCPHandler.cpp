#include <algorithm>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>
#include <base/types.h>
#include <Access/Credentials.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Formats/FormatFactory.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/LimitReadBuffer.h>
#include <IO/Progress.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/Session.h>
#include <Access/LocalApiToken.h>
#include <Interpreters/TablesStatus.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Server/TCPServer.h>
#include <Storages/Stream/StorageStream.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/StorageS3Cluster.h>
#include <base/scope_guard.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/NetException.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>

#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sinks/SinkToStorage.h>

#include <Core/Protocol.h>
#include <Storages/MergeTree/RequestResponse.h>

#include <Server/TCPHandler.h>

#include <Common/config_version.h>

#include <fmt/format.h>

#include <base/sleep.h>

/// proton: starts
#include <Bootstrap/Globals.h>
/// proton: ends


using namespace std::literals;
using namespace DB;

namespace CurrentMetrics
{
    extern const Metric QueryThread;
    extern const Metric ReadTaskRequestsSent;
    extern const Metric MergeTreeReadTaskRequestsSent;
    extern const Metric MergeTreeAllRangesAnnouncementsSent;
}

namespace ProfileEvents
{
    extern const Event ReadTaskRequestsSent;
    extern const Event MergeTreeReadTaskRequestsSent;
    extern const Event MergeTreeAllRangesAnnouncementsSent;
    extern const Event ReadTaskRequestsSentElapsedMicroseconds;
    extern const Event MergeTreeReadTaskRequestsSentElapsedMicroseconds;
    extern const Event MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds;
}

namespace DB::ErrorCodes
{
    extern const int ABORTED;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int AUTHENTICATION_FAILED;
    extern const int CLIENT_HAS_CONNECTED_TO_WRONG_PORT;
    extern const int CLIENT_INFO_DOES_NOT_MATCH;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TIMEOUT_EXCEEDED;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_EXCEPTION;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNKNOWN_PROTOCOL;
    extern const int UNSUPPORTED_METHOD;
    extern const int USER_EXPIRED;

    // We have to distinguish the case when query is killed by `KILL QUERY` statement
    // and when it is killed by `Protocol::Client::Cancel` packet.

    // When query is killed by `KILL QUERY` statement we have to end the execution
    // and send the exception to the actual client which initiated the TCP connection.

    // When query is killed by `Protocol::Client::Cancel` packet we just stop execution,
    // there is no need to send the exception which has been caused by the cancel packet.
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
}

namespace
{
NameToNameMap convertToQueryParameters(const Settings & passed_params)
{
    NameToNameMap query_parameters;
    for (const auto & param : passed_params)
    {
        std::string value;
        ReadBufferFromOwnString buf(param.getValueString());
        readQuoted(value, buf);
        query_parameters.emplace(param.getName(), value);
    }
    return query_parameters;
}

struct TurnOffBoolSettingTemporary
{
    bool & setting;
    bool prev_val;

    explicit TurnOffBoolSettingTemporary(bool & setting_)
        : setting(setting_)
        , prev_val(setting_)
    {
        if (prev_val)
            setting = false;
    }

    ~TurnOffBoolSettingTemporary()
    {
        if (prev_val)
            setting = true;
    }
};

}

namespace DB
{

TCPHandler::TCPHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    bool parse_proxy_protocol_,
    std::string server_display_name_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_,
    bool snapshot_mode_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , parse_proxy_protocol(parse_proxy_protocol_)
    , snapshot_mode(snapshot_mode_)
    , log(getLogger("TCPHandler"))
    , read_event(read_event_)
    , write_event(write_event_)
    , server_display_name(std::move(server_display_name_))
{
}


TCPHandler::~TCPHandler() = default;


void TCPHandler::runImpl()
{
    setThreadName("TCPHandler");
    ThreadStatus thread_status;

    extractConnectionSettingsFromContext(server.context());

    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(send_timeout);
    socket().setNoDelay(true);

    in = std::make_shared<ReadBufferFromPocoSocket>(socket(), read_event);

    /// Support for PROXY protocol
    if (parse_proxy_protocol && !receiveProxyHeader())
        return;

    if (in->eof())
    {
        LOG_INFO(log, "Client has not sent any data.");
        return;
    }

    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket(), write_event);

    /// User will be authenticated here. It will also set settings from user profile into connection_context.
    try
    {
        receiveHello();

        /// In interserver mode queries are executed without a session context.
        if (!is_interserver_mode)
            session->makeSessionContext();

        sendHello();

        if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM)
            receiveAddendum();

        if (!is_interserver_mode)
        {
            /// If session created, then settings in session context has been updated.
            /// So it's better to update the connection settings for flexibility.
            extractConnectionSettingsFromContext(session->sessionContext());

            /// When connecting, the default database could be specified.
            if (!default_database.empty())
                session->sessionContext()->setCurrentDatabase(default_database);
        }
    }
    catch (const Exception & e) /// Typical for an incorrect username, password, or address.
    {
        if (e.code() == ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT)
        {
            LOG_DEBUG(log, "Client has connected to wrong port.");
            return;
        }

        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
        {
            LOG_INFO(log, "Client has gone away.");
            return;
        }

        try
        {
            /// We try to send error information to the client.
            sendException(e, send_exception_with_stack_trace);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        throw;
    }

    while (tcp_server.isOpen())
    {
        /// We don't really have session in interserver mode, new one is created for each query. It's better to reset it now.
        if (is_interserver_mode)
            session.reset();

        /// We are waiting for a packet from the client. Thus, every `poll_interval` seconds check whether we need to shut down.
        {
            Stopwatch idle_time;
            UInt64 timeout_us = std::min(poll_interval, idle_connection_timeout) * 1000000;

            while (tcp_server.isOpen() && !server.isCancelled() && !static_cast<ReadBufferFromPocoSocket &>(*in).poll(timeout_us))
            {
                const auto elapsed_seconds = idle_time.elapsedSeconds();

                if (elapsed_seconds > idle_connection_timeout)
                {
                    LOG_TRACE(log, "Closing idle connection");
                    return;
                }
            }

            /// If we need to shut down, or client disconnects.
            if (!tcp_server.isOpen() || server.isCancelled() || in->eof())
            {
                LOG_TEST(log, "Closing connection (open: {}, cancelled: {}, eof: {})", tcp_server.isOpen(), server.isCancelled(), in->eof());
                return;
            }
        }

        /** An exception during the execution of request (it must be sent over the network to the client).
         *  The client will be able to accept it, if it did not happen while sending another packet and the client has not disconnected yet.
         */
        std::unique_ptr<DB::Exception> exception;

        SCOPE_EXIT({
            if (exception)
            {
                if (exception->code() == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT)
                    LOG_INFO(log, getExceptionMessageAndPattern(*exception, send_exception_with_stack_trace));
                else
                    LOG_ERROR(log, getExceptionMessageAndPattern(*exception, send_exception_with_stack_trace));
            }
        });

        OpenTelemetry::TracingContextHolderPtr thread_trace_context;
        /// Initialized later. It has to be destroyed after query_state is destroyed.
        std::optional<CurrentThread::QueryScope> query_scope;
        /// QueryState should be cleared before QueryScope, since otherwise
        /// the MemoryTracker will be wrong for possible deallocations.
        /// (i.e. deallocations from the Aggregator with two-level aggregation)
        /// Also it resets socket's timeouts.
        std::optional<QueryState> query_state;

        try
        {
            /** If Query - process it. If Ping or Cancel - go back to the beginning.
             *  There may come settings for a separate query that modify `query_context`.
             *  It's possible to receive part uuids packet before the query, so then receivePacket has to be called twice.
             */
            if (!receivePacketsExpectQuery(query_state))
                continue;

            /** If part_uuids got received in previous packet, trying to read again.
              */
            if (part_uuids_to_ignore.has_value() && !receivePacketsExpectQuery(query_state))
                continue;

            chassert(query_state.has_value());

            /// Set up tracing context for this query on current thread
            thread_trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>("TCPHandler",
                query_state->query_context->getClientInfo().client_trace_context,
                query_state->query_context->getSettingsRef(),
                query_state->query_context->getOpenTelemetrySpanLog());

            /// proton: starts. Disable send_logs_level
            query_scope.emplace(query_state->query_context);
            /// [this]
            /// {
            ///         std::lock_guard lock(fatal_error_mutex);
            ///         sendLogs(query_state.value());
            /// }
            /// proton: ends

            /// If query received, then settings in query_context has been updated.
            /// So it's better to update the connection settings for flexibility.
            extractConnectionSettingsFromContext(query_state->query_context);

            /// Sync timeouts on client and server during current query to avoid dangling queries on server.
            /// It should be reset at the end of query.
            query_state->timeout_setter = std::make_unique<TimeoutSetter>(socket(), send_timeout, receive_timeout);

            SCOPE_EXIT(logQueryDuration(query_state.value()));

            /// Should we send internal logs to client?
            /// proton: starts. Disable send_logs_level
            // const auto client_logs_level = query_state->query_context->getSettingsRef()[Setting::send_logs_level];
            // if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_LOGS
            //     && client_logs_level != LogsLevel::none)
            // {
            //     query_state->logs_queue = std::make_shared<InternalTextLogsQueue>();
            //     query_state->logs_queue->max_priority = Poco::Logger::parseLevel(client_logs_level.toString());
            //     CurrentThread::attachInternalTextLogsQueue(query_state->logs_queue, client_logs_level);
            // }
            /// proton: ends.
            if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
            {
                query_state->profile_queue = std::make_shared<InternalProfileEventsQueue>(std::numeric_limits<int>::max());
                CurrentThread::attachInternalProfileEventsQueue(query_state->profile_queue);
            }

            query_state->query_context->setExternalTablesInitializer([this, &query_state] (ContextPtr context)
            {
                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in external tables initializer");

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                /// Get blocks of temporary tables
                readTemporaryTables(query_state.value());

                /// Reset the input stream, as we received an empty block while receiving external table data.
                /// So, the stream has been marked as cancelled and we can't read from it anymore.
                query_state->block_in.reset();
                query_state->maybe_compressed_in.reset(); /// For more accurate accounting by MemoryTracker.
            });

            /// Send structure of columns to client for function input()
            query_state->query_context->setInputInitializer([this, &query_state] (ContextPtr context, const StoragePtr & input_storage)
            {

                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in Input initializer");

                auto metadata_snapshot = input_storage->getInMemoryMetadataPtr();

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                query_state->need_receive_data_for_input = true;

                /// Send ColumnsDescription for input storage.
                if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA
                    && query_state->query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
                {
                    sendTableColumns(query_state.value(), metadata_snapshot->getColumns());
                }

                /// Send block to the client - input storage structure.
                query_state->input_header = metadata_snapshot->getSampleBlock();
                sendData(query_state.value(), query_state->input_header);

                /// Update flag after reading external tables
                query_state->read_all_data = false;
            });

            query_state->query_context->setInputBlocksReaderCallback([this, &query_state] (ContextPtr context) -> Block
            {
                if (context != query_state->query_context)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected context in InputBlocksReader");

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                if (receivePacketsExpectData(query_state.value()))
                    return query_state->block_for_input;

                query_state->block_in.reset();
                query_state->maybe_compressed_in.reset();
                return {};
            });

            customizeContext(query_state->query_context);

            /// This callback is needed for requesting read tasks inside pipeline for distributed processing
            query_state->query_context->setReadTaskCallback([this, &query_state]() -> String
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::ReadTaskRequestsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                sendReadTaskRequest();

                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSent);

                auto res = receiveReadTaskResponse(query_state.value());

                ProfileEvents::increment(ProfileEvents::ReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());

                return res;
            });

            query_state->query_context->setMergeTreeAllRangesCallback([this, &query_state](InitialAllRangesAnnouncement announcement)
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeAllRangesAnnouncementsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(query_state.value());

                sendMergeTreeAllRangesAnnouncement(query_state.value(), announcement);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSent);
                ProfileEvents::increment(ProfileEvents::MergeTreeAllRangesAnnouncementsSentElapsedMicroseconds, watch.elapsedMicroseconds());
            });

            query_state->query_context->setMergeTreeReadTaskCallback([this, &query_state](ParallelReadRequest request) -> std::optional<ParallelReadResponse>
            {
                Stopwatch watch;
                CurrentMetrics::Increment callback_metric_increment(CurrentMetrics::MergeTreeReadTaskRequestsSent);

                std::lock_guard lock(callback_mutex);

                checkIfQueryCanceled(*query_state);

                sendMergeTreeReadTaskRequest(std::move(request));

                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSent);
                auto res = receivePartitionMergeTreeReadTaskResponse(query_state.value());
                ProfileEvents::increment(ProfileEvents::MergeTreeReadTaskRequestsSentElapsedMicroseconds, watch.elapsedMicroseconds());
                return res;
            });

            /// Processing Query
            query_state->io = executeQuery(query_state->query, query_state->query_context, false, query_state->stage);

            after_check_cancelled.restart();
            after_send_progress.restart();

            if (query_state->io.pipeline.pushing())
            {
                /// FIXME: check explicitly that insert query suggests to receive data via native protocol,
                query_state->need_receive_data_for_insert = true;
                processInsertQuery(query_state.value());
                query_state->io.onFinish();
            }
            else if (query_state->io.pipeline.pulling())
            {
                processOrdinaryQuery(query_state.value());
                query_state->io.onFinish();
            }
            else if (query_state->io.pipeline.completed())
            {
                {
                    CompletedPipelineExecutor executor(query_state->io.pipeline);

                    /// Should not check for cancel in case of input.
                    if (!query_state->need_receive_data_for_input)
                    {
                        auto callback = [this, &query_state]()
                        {
                            std::lock_guard lock(callback_mutex);

                            receivePacketsExpectCancel(query_state.value());

                            if (query_state->stop_read_return_partial_result)
                                return true;

                            sendProgress(query_state.value());
                            sendSelectProfileEvents(query_state.value());
                            sendLogs(query_state.value());

                            return false;
                        };

                        executor.setCancelCallback(std::move(callback), interactive_delay / 1000);
                    }

                    executor.execute();
                }

                query_state->io.onFinish();

                /// Send final progress after calling onFinish(), since it will update the progress.
                ///
                /// NOTE: we cannot send Progress for regular INSERT (with VALUES)
                /// without breaking protocol compatibility, but it can be done
                /// by increasing revision.
                sendProgress(query_state.value());
                sendSelectProfileEvents(query_state.value());
            }
            else
            {
                query_state->io.onFinish();
            }

            /// Do it before sending end of stream, to have a chance to show log message in client.
            query_scope->logPeakMemoryUsage();

            sendLogs(query_state.value());
            sendEndOfStream(query_state.value());

            query_state->finalizeOut(out);
        }
        catch (const Exception & e)
        {
            exception.reset(e.clone());
        }
        catch (const Poco::Exception & e)
        {
            exception = std::make_unique<DB::Exception>(Exception::CreateFromPocoTag{}, e);
        }
// Server should die on std logic errors in debug, like with assert()
// or ErrorCodes::LOGICAL_ERROR. This helps catch these errors in tests.
#ifdef DEBUG_OR_SANITIZER_BUILD
        catch (const std::logic_error & e)
        {
            if (query_state.has_value())
                query_state->io.onException();
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
            sendException(*exception, send_exception_with_stack_trace);
            std::abort();
        }
#endif
        catch (const std::exception & e)
        {
            exception = std::make_unique<DB::Exception>(Exception::CreateFromSTDTag{}, e);
        }
        catch (...)
        {
            exception = std::make_unique<DB::Exception>(ErrorCodes::UNKNOWN_EXCEPTION, "Unknown exception");
        }

        if (exception)
        {
            auto exception_code = exception->code();

            if (!query_state.has_value())
                return;

            try
            {
                exception->rethrow();
            }
            catch (...)
            {
                query_state->io.onException();
            }

            if (exception_code == ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT)
            {
                query_state->cancelOut(out);
                return;
            }

            if (thread_trace_context)
                thread_trace_context->root_span.addAttribute(*exception);

            if (!out || out->isCanceled())
            {
                query_state->cancelOut(out);
                return;
            }

            try
            {
                std::lock_guard lock(callback_mutex);

                /// Try to send logs to client, but it could be risky too
                /// Assume that we can't break output here
                sendLogs(query_state.value());

                if (exception_code == ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT)
                    sendEndOfStream(query_state.value());
                else
                    sendException(*exception, send_exception_with_stack_trace);

                /// A query packet is always followed by one or more data packets.
                /// If some of those data packets are left, try to skip them.
                if (!query_state->read_all_data)
                    skipData(query_state.value());
            }
            catch (...)
            {
                query_state->cancelOut(out);
                tryLogCurrentException(log, "Can't send logs or exception to client. Close connection.");
                return;
            }

            if (exception->code() == ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT || exception->code() == ErrorCodes::USER_EXPIRED)
            {
                LOG_DEBUG(log, "Going to close connection due to exception: {}", exception->message());
                query_state->finalizeOut(out);
                return;
            }
            else
            {
                LOG_TRACE(log, "Logs and exception has been sent. The connection is preserved.");
            }
        }

        query_state->finalizeOut(out);
    }
}


void TCPHandler::logQueryDuration(QueryState & state)
{
    if (state.query_duration_already_logged)
        return;
    state.query_duration_already_logged = true;
    auto elapsed_sec = state.watch.elapsedSeconds();
    /// We already logged more detailed info if we read some rows
    if (elapsed_sec < 1.0 && state.progress.read_rows)
        return;
    LOG_DEBUG(log, "Processed in {} sec.", elapsed_sec);
}


void TCPHandler::extractConnectionSettingsFromContext(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    send_exception_with_stack_trace = settings.calculate_text_stack_trace;
    send_timeout = settings.send_timeout;
    receive_timeout = settings.receive_timeout;
    poll_interval = settings.poll_interval;
    idle_connection_timeout = settings.idle_connection_timeout;
    interactive_delay = settings.interactive_delay;
    sleep_in_send_tables_status = settings.sleep_in_send_tables_status_ms;
    unknown_packet_in_send_data = settings.unknown_packet_in_send_data;
    sleep_after_receiving_query = settings.sleep_after_receiving_query_ms;
}


bool TCPHandler::receivePacketsExpectQuery(std::optional<QueryState> & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Hello:
            processUnexpectedHello();

        case Protocol::Client::Data:
        case Protocol::Client::Scalar:
            processUnexpectedData();
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Data received from client");

        case Protocol::Client::Ping:
            writeVarUInt(Protocol::Server::Pong, *out);
            out->next();
            return false;

        case Protocol::Client::Cancel:
            return false;

        case Protocol::Client::TablesStatusRequest:
            processTablesStatusRequest();
            return false;

        case Protocol::Client::IgnoredPartUUIDs:
            /// Part uuids packet if any comes before query.
            processIgnoredPartUUIDs();
            return true;

        case Protocol::Client::Query:
            processQuery(state);
            return true;

        default:
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
    }
}


bool TCPHandler::receivePacketsExpectDataConcurrentWithExecutor(QueryState & state)
{
    std::lock_guard lock(callback_mutex);
    return receivePacketsExpectData(state);
}

bool TCPHandler::receivePacketsExpectData(QueryState & state)
{
    /// Poll interval should not be greater than receive_timeout
    constexpr UInt64 min_timeout_us = 5000; // 5 ms
    UInt64 timeout_us = std::max(
            min_timeout_us,
            std::min(
                poll_interval * 1000000,
                static_cast<UInt64>(receive_timeout.totalMicroseconds())));

    Stopwatch watch;

    while (!server.isCancelled() && tcp_server.isOpen())
    {
        while (!in->poll(timeout_us))
        {
            size_t elapsed = size_t(watch.elapsedSeconds());
            if (elapsed > size_t(receive_timeout.totalSeconds()))
            {
                throw NetException(ErrorCodes::SOCKET_TIMEOUT,
                                "Timeout exceeded while receiving data from client. Waited for {} seconds, timeout is {} seconds.",
                                elapsed, receive_timeout.totalSeconds());
            }
        }

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::IgnoredPartUUIDs:
                processUnexpectedIgnoredPartUUIDs();

            case Protocol::Client::Query:
                processUnexpectedQuery();

            case Protocol::Client::Hello:
                processUnexpectedHello();

            case Protocol::Client::TablesStatusRequest:
                processUnexpectedTablesStatusRequest();

            case Protocol::Client::Data:
            case Protocol::Client::Scalar:
            {
                bool empty_block;
                if (state.skipping_data)
                    empty_block = !processUnexpectedData();
                else
                    empty_block = !processData(state, packet_type == Protocol::Client::Scalar);
                if (empty_block)
                    state.read_all_data = true;
                return !empty_block;
            }

            case Protocol::Client::Ping:
                writeVarUInt(Protocol::Server::Pong, *out);
                out->next();
                continue;

            case Protocol::Client::Cancel:
                processCancel(state);
                return false; // We return false from this function as if no more data received

            default:
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet {} from client", toString(packet_type));
        }
    }

    chassert(server.isCancelled() || !tcp_server.isOpen());
    throw Exception(ErrorCodes::ABORTED, "Server shutdown is called");
}


void TCPHandler::readTemporaryTables(QueryState & state)
{
    sendLogs(state);

    /// no sense in partial_result_on_first_cancel setting when temporary data is read.
    auto off_setting_guard = TurnOffBoolSettingTemporary(state.allow_partial_result_on_first_cancel);

    while (receivePacketsExpectData(state))
    {
        sendLogs(state);
        sendInsertProfileEvents(state);
    }
}


void TCPHandler::skipData(QueryState & state)
{
    state.skipping_data = true;
    SCOPE_EXIT({ state.skipping_data = false; });

    size_t blocks = 0;
    while (receivePacketsExpectData(state))
        ++blocks;
    LOG_TRACE(log, "Discarded {} blocks", blocks);
}


void TCPHandler::startInsertQuery(QueryState & state)
{
    std::lock_guard lock(callback_mutex);

    /// Send ColumnsDescription for insertion table
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA)
    {
        const auto & table_id = state.query_context->getInsertionTable();
        if (state.query_context->getSettingsRef().input_format_defaults_for_omitted_fields)
        {
            if (!table_id.empty())
            {
                auto storage_ptr = DatabaseCatalog::instance().getTable(table_id, state.query_context);
                sendTableColumns(state, storage_ptr->getInMemoryMetadataPtr()->getColumns());
            }
        }
    }

    /// Send block to the client - table structure.
    sendData(state, state.io.pipeline.getHeader());
    sendLogs(state);

    /// Update flag after reading external tables
    state.read_all_data = false;
}


void TCPHandler::processInsertQuery(QueryState & state)
{
    size_t num_threads = state.io.pipeline.getNumThreads();

    auto run_executor = [&](auto & executor)
    {
        try
        {
            /// Made above the rest of the lines,
            /// so that in case of `start` function throws an exception,
            /// client receive exception before sending data.
            executor.start();

            startInsertQuery(state);

            while (receivePacketsExpectDataConcurrentWithExecutor(state))
            {
                executor.push(std::move(state.block_for_insert));

                sendLogs(state);
                sendInsertProfileEvents(state);
            }

            executor.finish();
        }
        catch (...)
        {
            executor.cancel();
            throw;
        }
    };

    if (num_threads > 1)
    {
        PushingAsyncPipelineExecutor executor(state.io.pipeline);
        run_executor(executor);
    }
    else
    {
        PushingPipelineExecutor executor(state.io.pipeline);
        run_executor(executor);
    }

    sendInsertProfileEvents(state);
}


void TCPHandler::processOrdinaryQuery(QueryState & state)
{
    auto & pipeline = state.io.pipeline;

    if (state.query_context->getSettingsRef().allow_experimental_query_deduplication)
    {
        sendPartUUIDs(state);
    }

    /// Send header-block, to allow client to prepare output format for data to send.
    {
        const auto & header = pipeline.getHeader();

        if (header)
        {
            sendData(state, header);
        }
    }

    {
        PullingAsyncPipelineExecutor executor(pipeline);
        CurrentMetrics::Increment query_thread_metric_increment{CurrentMetrics::QueryThread};

        try
        {
            Block block;
            while (executor.pull(block, interactive_delay / 1000))
            {
                {
                    std::lock_guard lock(callback_mutex);
                    receivePacketsExpectCancel(state);
                }

                {
                    std::lock_guard lock(callback_mutex);

                    if (after_send_progress.elapsed() / 1000 >= interactive_delay)
                    {
                        /// Some time passed and there is a progress.
                        after_send_progress.restart();
                        sendProgress(state);
                        sendSelectProfileEvents(state);
                    }

                    sendLogs(state);

                    if (block)
                    {
                        if (!state.io.null_format)
                            sendData(state, block);
                    }
                }
            }
        }
        catch (...)
        {
            executor.cancel();
            throw;
        }

        /** If data has run out, we will send the profiling data and total values to
          * the last zero block to be able to use
          * this information in the suffix output of stream.
          * If the request was interrupted, then `sendTotals` and other methods could not be called,
          *  because we have not read all the data yet,
          *  and there could be ongoing calculations in other threads at the same time.
          */


        std::lock_guard lock(callback_mutex);

        receivePacketsExpectCancel(state);

        sendTotals(state, executor.getTotalsBlock());
        sendExtremes(state, executor.getExtremesBlock());
        sendProfileInfo(state, executor.getProfileInfo());
        sendProgress(state);
        sendLogs(state);
        sendSelectProfileEvents(state);

        sendData(state, {});

        sendProgress(state);
    }
}


void TCPHandler::processTablesStatusRequest()
{
    TablesStatusRequest request;
    request.read(*in, client_tcp_protocol_version);

    ContextPtr context_to_resolve_table_names = (session && session->sessionContext()) ? session->sessionContext() : server.context();

    TablesStatusResponse response;
    for (const QualifiedTableName & table_name: request.tables)
    {
        auto resolved_id = context_to_resolve_table_names->tryResolveStorageID({table_name.database, table_name.table});
        StoragePtr table = DatabaseCatalog::instance().tryGetTable(resolved_id, context_to_resolve_table_names);
        if (!table)
            continue;

        TableStatus status;
        if (auto * stream = dynamic_cast<StorageStream *>(table.get())) /// proton: starts
        {
            /// If it is just a virtual table, no table status
            if (stream->isRemote())
                continue;

            /// Please note for Stream, the absolute delay means the last 32 bits
            /// of the last sequence number it commits
            /// FIXME, revise the protocol to support multiple shard
            status.is_replicated = true;
            /// status.absolute_delay = distributed_merge_tree->lastSN() & 0XFFFFFFFF;
            /// auto shard_sns = stream->lastCommittedSequences();
            status.absolute_delay = 0;
        } /// proton: ends
        else
            status.is_replicated = false;

        response.table_states_by_id.emplace(table_name, std::move(status));
    }

    writeVarUInt(Protocol::Server::TablesStatusResponse, *out);

    /// For testing hedged requests
    if (unlikely(sleep_in_send_tables_status.totalMilliseconds()))
    {
        out->next();
        std::chrono::milliseconds ms(sleep_in_send_tables_status.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }

    response.write(*out, client_tcp_protocol_version);

    out->next();
}


void TCPHandler::processUnexpectedTablesStatusRequest()
{
    TablesStatusRequest skip_request;
    skip_request.read(*in, client_tcp_protocol_version);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet TablesStatusRequest received from client");
}


void TCPHandler::sendPartUUIDs(QueryState & state)
{
    auto uuids = state.query_context->getPartUUIDs()->get();
    if (uuids.empty())
        return;

    writeVarUInt(Protocol::Server::PartUUIDs, *out);
    writeVectorBinary(uuids, *out);

    out->next();
}


void TCPHandler::sendReadTaskRequest()
{
    writeVarUInt(Protocol::Server::ReadTaskRequest, *out);
    out->next();
}


void TCPHandler::sendMergeTreeAllRangesAnnouncement(QueryState &, InitialAllRangesAnnouncement announcement)
{
    writeVarUInt(Protocol::Server::MergeTreeAllRangesAnnouncement, *out);
    announcement.serialize(*out);
    out->next();
}


void TCPHandler::sendMergeTreeReadTaskRequest(ParallelReadRequest request)
{
    writeVarUInt(Protocol::Server::MergeTreeReadTaskRequest, *out);
    request.serialize(*out);
    out->next();
}


void TCPHandler::sendProfileInfo(QueryState &, const ProfileInfo & info)
{
    writeVarUInt(Protocol::Server::ProfileInfo, *out);
    info.write(*out);

    out->next();
}


void TCPHandler::sendTotals(QueryState & state, const Block & totals)
{
    if (!totals)
        return;

    initBlockOutput(state, totals);

    writeVarUInt(Protocol::Server::Totals, *out);
    writeStringBinary("", *out);

    state.block_out->write(totals);
    state.maybe_compressed_out->next();
    out->next();
}


void TCPHandler::sendExtremes(QueryState & state, const Block & extremes)
{
    if (!extremes)
        return;

    initBlockOutput(state, extremes);

    writeVarUInt(Protocol::Server::Extremes, *out);
    writeStringBinary("", *out);

    state.block_out->write(extremes);
    state.maybe_compressed_out->next();
    out->next();
}


void TCPHandler::sendProfileEvents(QueryState & state)
{
    Stopwatch stopwatch;
    Block block = ProfileEvents::getProfileEvents(server_display_name, state.profile_queue, state.last_sent_snapshots);
    if (block.rows() != 0)
    {
        initProfileEventsBlockOutput(state, block);

        writeVarUInt(Protocol::Server::ProfileEvents, *out);
        writeStringBinary("", *out);

        state.profile_events_block_out->write(block);
        state.profile_events_block_out->flush();
        out->next();

        auto elapsed_milliseconds = stopwatch.elapsedMilliseconds();
        if (elapsed_milliseconds > 100)
            LOG_DEBUG(log, "Sending profile events block with {} rows, {} bytes took {} milliseconds",
                block.rows(), block.bytes(), elapsed_milliseconds);
    }
}


void TCPHandler::sendSelectProfileEvents(QueryState & state)
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
        return;

    sendProfileEvents(state);
}


void TCPHandler::sendInsertProfileEvents(QueryState & state)
{
    if (client_tcp_protocol_version < DBMS_MIN_PROTOCOL_VERSION_WITH_PROFILE_EVENTS_IN_INSERT)
        return;

    sendProfileEvents(state);
}


bool TCPHandler::receiveProxyHeader()
{
    if (in->eof())
    {
        LOG_WARNING(log, "Client has not sent any data.");
        return false;
    }

    String forwarded_address;

    /// Only PROXYv1 is supported.
    /// Validation of protocol is not fully performed.

    LimitReadBuffer limit_in(*in, {.read_no_more=107, .expect_eof=true}); /// Maximum length from the specs.

    assertString("PROXY ", limit_in);

    if (limit_in.eof())
    {
        LOG_WARNING(log, "Incomplete PROXY header is received.");
        return false;
    }

    /// TCP4 / TCP6 / UNKNOWN
    if ('T' == *limit_in.position())
    {
        assertString("TCP", limit_in);

        if (limit_in.eof())
        {
            LOG_WARNING(log, "Incomplete PROXY header is received.");
            return false;
        }

        if ('4' != *limit_in.position() && '6' != *limit_in.position())
        {
            LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
            return false;
        }

        ++limit_in.position();
        assertChar(' ', limit_in);

        /// Read the first field and ignore other.
        readStringUntilWhitespace(forwarded_address, limit_in);

        /// Skip until \r\n
        while (!limit_in.eof() && *limit_in.position() != '\r')
            ++limit_in.position();
        assertString("\r\n", limit_in);
    }
    else if (checkString("UNKNOWN", limit_in))
    {
        /// This is just a health check, there is no subsequent data in this connection.

        while (!limit_in.eof() && *limit_in.position() != '\r')
            ++limit_in.position();
        assertString("\r\n", limit_in);
        return false;
    }
    else
    {
        LOG_WARNING(log, "Unexpected protocol in PROXY header is received.");
        return false;
    }

    LOG_TRACE(log, "Forwarded client address from PROXY header: {}", forwarded_address);
    forwarded_for = std::move(forwarded_address);
    return true;
}


namespace
{

std::string formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(const Poco::Util::AbstractConfiguration& config)
{
    std::string result = fmt::format(
        "HTTP/1.0 400 Bad Request\r\n\r\n"
        "Port {} is for clickhouse-client program\r\n",
        config.getString("tcp_port"));

    if (config.has("http_port"))
    {
        result += fmt::format(
            "You must use port {} for HTTP.\r\n",
            config.getString("http_port"));
    }

    return result;
}

}


std::unique_ptr<Session> TCPHandler::makeSession()
{
    auto interface = is_interserver_mode ? ClientInfo::Interface::TCP_INTERSERVER : ClientInfo::Interface::TCP;

    auto res = std::make_unique<Session>(server.context(), interface);

    auto & client_info = res->getClientInfo();
    client_info.forwarded_for = forwarded_for;
    client_info.client_name = client_name;
    client_info.client_version_major = client_version_major;
    client_info.client_version_minor = client_version_minor;
    client_info.client_version_patch = client_version_patch;
    client_info.client_tcp_protocol_version = client_tcp_protocol_version;

    client_info.connection_client_version_major = client_version_major;
    client_info.connection_client_version_minor = client_version_minor;
    client_info.connection_client_version_patch = client_version_patch;
    client_info.connection_tcp_protocol_version = client_tcp_protocol_version;

    client_info.quota_key = quota_key;
    client_info.interface = interface;

    return res;
}


void TCPHandler::receiveHello()
{
    /// Receive `hello` packet.
    UInt64 packet_type = 0;
    String user;
    String password;

    readVarUInt(packet_type, *in);

    if (packet_type != Protocol::Client::Hello)
    {
        /** If you accidentally accessed the HTTP protocol for a port destined for an internal TCP protocol,
          * Then instead of the packet type, there will be G (GET) or P (POST), in most cases.
          */
        if (packet_type == 'G' || packet_type == 'P')
        {
            writeString(formatHTTPErrorResponseWhenUserIsConnectedToWrongPort(server.config()), *out);
            out->next();
            throw Exception(ErrorCodes::CLIENT_HAS_CONNECTED_TO_WRONG_PORT, "Client has connected to wrong port");
        }
        else
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                               "Unexpected packet from client (expected Hello, got {})", packet_type);
    }

    readStringBinary(client_name, *in);
    readVarUInt(client_version_major, *in);
    readVarUInt(client_version_minor, *in);
    // NOTE For backward compatibility of the protocol, client cannot send its version_patch.
    readVarUInt(client_tcp_protocol_version, *in);
    readStringBinary(default_database, *in);
    readStringBinary(user, *in);
    readStringBinary(password, *in);

    if (user.empty())
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet from client (no user in Hello package)");

    /// The local API user may only connect from loopback — reject all other origins
    /// before authentication so the token is never tested against a remote attempt.
    if (LocalApiToken::isLocalApiTokenUser(user))
    {
        if (!socket().peerAddress().host().isLoopback())
            throw Exception(
                ErrorCodes::AUTHENTICATION_FAILED,
                "User '{}' is restricted to localhost connections",
                user);
    }

    LOG_DEBUG(log, "Connected {} version {}.{}.{}, revision: {}{}{}.",
        client_name,
        client_version_major, client_version_minor, client_version_patch,
        client_tcp_protocol_version,
        (!default_database.empty() ? ", database: " + default_database : ""),
        (!user.empty() ? ", user: " + user : "")
    );

    is_interserver_mode = (user == USER_INTERSERVER_MARKER) && password.empty();
    if (is_interserver_mode)
    {
        processClusterNameAndSalt();
        return;
    }

    session = makeSession();
    session->authenticate(user, password, socket().peerAddress());
}


void TCPHandler::receiveAddendum()
{
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_QUOTA_KEY)
    {
        readStringBinary(quota_key, *in);
        if (!is_interserver_mode)
            session->getClientInfo().quota_key = quota_key;
    }
}


void TCPHandler::processUnexpectedHello()
{
    UInt64 skip_uint_64;
    String skip_string;

    readStringBinary(skip_string, *in);
    readVarUInt(skip_uint_64, *in);
    readVarUInt(skip_uint_64, *in);
    readVarUInt(skip_uint_64, *in);
    readStringBinary(skip_string, *in);
    readStringBinary(skip_string, *in);
    readStringBinary(skip_string, *in);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Hello received from client");
}


void TCPHandler::sendHello()
{
    writeVarUInt(Protocol::Server::Hello, *out);
    writeStringBinary(VERSION_NAME, *out);
    writeVarUInt(VERSION_MAJOR, *out);
    writeVarUInt(VERSION_MINOR, *out);
    writeVarUInt(DBMS_TCP_PROTOCOL_VERSION, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE)
        writeStringBinary(DateLUT::instance().getTimeZone(), *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME)
        writeStringBinary(server_display_name, *out);
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_VERSION_PATCH)
        writeVarUInt(VERSION_PATCH, *out);

    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2)
    {
        chassert(!nonce.has_value());
        /// Contains lots of stuff (including time), so this should be enough for NONCE.
        nonce.emplace(thread_local_rng());
        writeIntBinary(nonce.value(), *out);
    }
    else
    {
        LOG_WARNING(LogFrequencyLimiter(log, 10),
                    "Using deprecated interserver protocol because the client is too old. Consider upgrading all nodes in cluster.");
    }

    out->next();
}


void TCPHandler::processIgnoredPartUUIDs()
{
    readVectorBinary(part_uuids_to_ignore.emplace(), *in);
}


void TCPHandler::processUnexpectedIgnoredPartUUIDs()
{
    std::vector<UUID> skip_part_uuids;
    readVectorBinary(skip_part_uuids, *in);
    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet IgnoredPartUUIDs received from client");
}


String TCPHandler::receiveReadTaskResponse(QueryState & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Cancel:
            processCancel(state);
            return {};

        case Protocol::Client::ReadTaskResponse:
        {
            UInt64 version = 0;
            readVarUInt(version, *in);
            if (version != DBMS_CLUSTER_PROCESSING_PROTOCOL_VERSION)
                throw Exception(ErrorCodes::UNKNOWN_PROTOCOL, "Protocol version for distributed processing mismatched");
            String response;
            readStringBinary(response, *in);
            return response;
        }

        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Received {} packet after requesting read task",
                    Protocol::Client::toString(packet_type));
    }
}


std::optional<ParallelReadResponse> TCPHandler::receivePartitionMergeTreeReadTaskResponse(QueryState & state)
{
    UInt64 packet_type = 0;
    readVarUInt(packet_type, *in);

    switch (packet_type)
    {
        case Protocol::Client::Cancel:
            processCancel(state);
            return {};

        case Protocol::Client::MergeTreeReadTaskResponse:
        {
            ParallelReadResponse response;
            response.deserialize(*in);
            return response;
        }

        default:
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Received {} packet after requesting read task",
                Protocol::Client::toString(packet_type));
    }
}


void TCPHandler::processClusterNameAndSalt()
{
    readStringBinary(cluster, *in);
    readStringBinary(salt, *in, 32);
}


void TCPHandler::processQuery(std::optional<QueryState> & state)
{
    UInt64 stage = 0;
    UInt64 compression = 0;

    state.emplace();

    if (part_uuids_to_ignore.has_value())
        state->part_uuids_to_ignore = std::move(part_uuids_to_ignore);

    readStringBinary(state->query_id, *in);

    /// In interserver mode,
    /// initial_user can be empty in case of Distributed INSERT via Buffer/Kafka,
    /// (i.e. when the INSERT is done with the global context without user),
    /// so it is better to reset session to avoid using old user.
    if (is_interserver_mode)
    {
        session = makeSession();
    }

    /// Read client info.
    ClientInfo client_info = session->getClientInfo();
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        client_info.read(*in, client_tcp_protocol_version);

    /// Per query settings are also passed via TCP.
    /// We need to check them before applying due to they can violate the settings constraints.
     auto settings_format = (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS)
        ? SettingsWriteFormat::STRINGS_WITH_FLAGS
        : SettingsWriteFormat::BINARY;

    Settings passed_settings;
    passed_settings.read(*in, settings_format);

    /// Interserver secret.
    std::string received_hash;
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET)
    {
        readStringBinary(received_hash, *in, 32);
    }

    readVarUInt(stage, *in);
    state->stage = QueryProcessingStage::Enum(stage);

    readVarUInt(compression, *in);
    state->compression = static_cast<Protocol::Compression>(compression);
    last_block_in.compression = state->compression;

    readStringBinary(state->query, *in);

    Settings passed_params;
    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
        passed_params.read(*in, settings_format);

    if (is_interserver_mode)
    {
        client_info.interface = ClientInfo::Interface::TCP_INTERSERVER;
#if USE_SSL
        String cluster_secret = Globals::getServerDescriptor().secret;

        if (salt.empty() || cluster_secret.empty())
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed (no salt/cluster secret)");
            session->onAuthenticationFailure(/*user_name=*/ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 && !nonce.has_value())
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed (no nonce)");
            session->onAuthenticationFailure(/*user_name=*/ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        std::string data(salt);
        // For backward compatibility
        if (nonce.has_value())
            data += std::to_string(nonce.value());
        data += cluster_secret;
        data += state->query;
        data += state->query_id;
        data += client_info.initial_user;

        std::string calculated_hash = encodeSHA256(data);
        assert(calculated_hash.size() == 32);

        /// TODO maybe also check that peer address actually belongs to the cluster?
        if (calculated_hash != received_hash)
        {
            auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED, "Interserver authentication failed");
            session->onAuthenticationFailure(/* user_name */ std::nullopt, socket().peerAddress(), exception);
            throw exception; /// NOLINT
        }

        /// NOTE Usually we get some fields of client_info (including initial_address and initial_user) from user input,
        /// so we should not rely on that. However, in this particular case we got client_info from other clickhouse-server, so it's ok.
        if (client_info.initial_user.empty())
        {
            LOG_DEBUG(log, "User (no user, interserver mode)");
        }
        else
        {
            LOG_DEBUG(log, "User (initial, interserver mode): {}", client_info.initial_user);
            session->authenticate(AlwaysAllowCredentials{client_info.initial_user}, *client_info.initial_address);
        }
#else
        auto exception = Exception(ErrorCodes::AUTHENTICATION_FAILED,
            "Inter-server secret support is disabled, because proton was built without SSL library");
        session->onAuthenticationFailure(/* user_name */ std::nullopt, socket().peerAddress(), exception);
        throw exception; /// NOLINT
#endif
    }

    state->query_context = session->makeQueryContext(client_info);

    /// Sets the default database if it wasn't set earlier for the session context.
    if (is_interserver_mode && !default_database.empty())
        state->query_context->setCurrentDatabase(default_database);

    if (state->part_uuids_to_ignore)
        state->query_context->getIgnoredPartUUIDs()->add(*state->part_uuids_to_ignore);

    state->query_context->setProgressCallback(
        [this, &state] (const Progress & value) { this->updateProgress(state.value(), value); });
    state->query_context->setFileProgressCallback(
        [this, &state](const FileProgress & value) { this->updateProgress(state.value(), Progress(value)); });
    ///
    /// Settings
    ///
    auto settings_changes = passed_settings.changes();
    auto query_kind = state->query_context->getClientInfo().query_kind;
    if (query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        /// Throw an exception if the passed settings violate the constraints.
        state->query_context->checkSettingsConstraints(settings_changes);
    }
    else
    {
        /// Quietly clamp to the constraints if it's not an initial query.
        state->query_context->clampToSettingsConstraints(settings_changes);
    }
    state->query_context->applySettingsChanges(settings_changes);
    
    /// proton: starts
    /// set query_mode to 'snapshot' if snapshot_mode is on
    if (snapshot_mode)
        state->query_context->setSetting("query_mode", Field{"snapshot"});
    /// proton: ends

    /// Use the received query id, or generate a random default. It is convenient
    /// to also generate the default OpenTelemetry trace id at the same time, and
    /// set the trace parent.
    /// Notes:
    /// 1) ClientInfo might contain upstream trace id, so we decide whether to use
    /// the default ids after we have received the ClientInfo.
    /// 2) There is the opentelemetry_start_trace_probability setting that
    /// controls when we start a new trace. It can be changed via Native protocol,
    /// so we have to apply the changes first.
    state->query_context->setCurrentQueryId(state->query_id);

    state->query_context->addQueryParameters(convertToQueryParameters(passed_params));

    state->allow_partial_result_on_first_cancel = state->query_context->getSettingsRef().partial_result_on_first_cancel;

    /// For testing hedged requests
    if (unlikely(sleep_after_receiving_query.totalMilliseconds()))
    {
        std::chrono::milliseconds ms(sleep_after_receiving_query.totalMilliseconds());
        std::this_thread::sleep_for(ms);
    }

    state->read_all_data = false;
}

void TCPHandler::processUnexpectedQuery()
{
    UInt64 skip_uint_64;
    String skip_string;

    readStringBinary(skip_string, *in);

    ClientInfo skip_client_info;
    if (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_CLIENT_INFO)
        skip_client_info.read(*in, client_tcp_protocol_version);

    Settings skip_settings;
    auto settings_format = (client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS) ? SettingsWriteFormat::STRINGS_WITH_FLAGS
                                                                                                      : SettingsWriteFormat::BINARY;
    skip_settings.read(*in, settings_format);

    std::string skip_hash;
    bool interserver_secret = client_tcp_protocol_version >= DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET;
    if (interserver_secret)
        readStringBinary(skip_hash, *in, 32);

    readVarUInt(skip_uint_64, *in);

    readVarUInt(skip_uint_64, *in);
    last_block_in.compression = static_cast<Protocol::Compression>(skip_uint_64);

    readStringBinary(skip_string, *in);

    if (client_tcp_protocol_version >= DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
        skip_settings.read(*in, settings_format);

    throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet Query received from client");
}

bool TCPHandler::processData(QueryState & state, bool scalar)
{
    initBlockInput(state);

    /// The name of the temporary table for writing data, default to empty string
    auto temporary_id = StorageID::createEmpty();
    readStringBinary(temporary_id.table_name, *in);

    /// Read one block from the network and write it down
    Block block = state.block_in->read();

    if (!block)
        return false;

    if (scalar)
    {
        /// Scalar value
        state.query_context->addScalar(temporary_id.table_name, block);
    }
    else if (!state.need_receive_data_for_insert && !state.need_receive_data_for_input)
    {
        /// Data for external tables

        auto resolved = state.query_context->tryResolveStorageID(temporary_id, Context::ResolveExternal);
        StoragePtr storage;
        /// If such a table does not exist, create it.
        if (resolved)
        {
            storage = DatabaseCatalog::instance().getTable(resolved, state.query_context);
        }
        else
        {
            NamesAndTypesList columns = block.getNamesAndTypesList();
            auto temporary_table = TemporaryTableHolder(state.query_context, ColumnsDescription{columns}, {});
            storage = temporary_table.getTable();
            state.query_context->addExternalTable(temporary_id.table_name, std::move(temporary_table));
        }
        auto metadata_snapshot = storage->getInMemoryMetadataPtr();
        /// The data will be written directly to the table.
        QueryPipeline temporary_table_out(storage->write(ASTPtr(), metadata_snapshot, state.query_context));
        PushingPipelineExecutor executor(temporary_table_out);
        executor.start();
        executor.push(block);
        executor.finish();
    }
    else if (state.need_receive_data_for_input)
    {
        /// 'input' table function.
        state.block_for_input = block;
    }
    else
    {
        /// INSERT query.
        state.block_for_insert = block;
    }

    return true;
}


bool TCPHandler::processUnexpectedData()
{
    String skip_external_table_name;
    readStringBinary(skip_external_table_name, *in);

    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    if (last_block_in.compression == Protocol::Compression::Enable)
        maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true);
    else
        maybe_compressed_in = in;

    auto skip_block_in = std::make_shared<NativeReader>(*maybe_compressed_in, client_tcp_protocol_version);
    bool empty_block = !skip_block_in->read();
    return !empty_block;
}


void TCPHandler::initBlockInput(QueryState & state)
{
    if (!state.block_in)
    {
        /// 'allow_different_codecs' is set to true, because some parts of compressed data can be precompressed in advance
        /// with another codec that the rest of the data. Example: data sent by Distributed tables.

        if (state.compression == Protocol::Compression::Enable)
            state.maybe_compressed_in = std::make_shared<CompressedReadBuffer>(*in, /* allow_different_codecs */ true);
        else
            state.maybe_compressed_in = in;

        Block header;
        if (state.io.pipeline.pushing())
            header = state.io.pipeline.getHeader();
        else if (state.need_receive_data_for_input)
            header = state.input_header;

        state.block_in = std::make_unique<NativeReader>(
            *state.maybe_compressed_in,
            header,
            client_tcp_protocol_version,
            getFormatSettings(state.query_context));
    }
}


CompressionCodecPtr TCPHandler::getCompressionCodec(const Settings & query_settings, Protocol::Compression compression)
{
    std::string method = Poco::toUpper(query_settings.network_compression_method.toString());
    std::optional<int> level;

    if (method == "ZSTD")
        level = query_settings.network_zstd_compression_level;

    if (compression == Protocol::Compression::Enable)
    {
        CompressionCodecFactory::instance().validateCodec(
            method,
            level,
            !query_settings.allow_suspicious_codecs,
            query_settings.allow_experimental_codecs);
        return CompressionCodecFactory::instance().get(method, level);
    }

    return nullptr;
}


void TCPHandler::initMaybeCompressedOut(QueryState & state)
{
    const Settings & query_settings = state.query_context->getSettingsRef();
    if (!state.maybe_compressed_out)
    {
        if (auto codec = getCompressionCodec(query_settings, state.compression))
            state.maybe_compressed_out = std::make_shared<CompressedWriteBuffer>(*out, codec);
        else
            state.maybe_compressed_out = out;
    }
}


void TCPHandler::initBlockOutput(QueryState & state, const Block & block)
{
    if (!state.block_out)
    {
        initMaybeCompressedOut(state);

        const Settings & query_settings = state.query_context->getSettingsRef();
        state.block_out = std::make_unique<NativeWriter>(
            *state.maybe_compressed_out,
            client_tcp_protocol_version,
            block.cloneEmpty(),
            getFormatSettings(state.query_context),
            !query_settings.low_cardinality_allow_in_native_format);

        /// proton: starts.
        /// If client is other timeplus, we can enable internal channel to send more additional content,
        /// such as `watermark` or `sequence number` etc.
        if (state.query_context->getClientInfo().client_name.starts_with(VERSION_NAME))
            state.block_out->enableInternalChannel();
        /// proton: ends.
    }
}


void TCPHandler::initLogsBlockOutput(QueryState & state, const Block & block)
{
    if (!state.logs_block_out)
    {
        /// Use uncompressed stream since log blocks usually contain only one row
        const Settings & query_settings = state.query_context->getSettingsRef();
        state.logs_block_out = std::make_unique<NativeWriter>(
            *out, client_tcp_protocol_version, block.cloneEmpty(), getFormatSettings(state.query_context), !query_settings.low_cardinality_allow_in_native_format);
    }
}


void TCPHandler::initProfileEventsBlockOutput(QueryState & state, const Block & block)
{
    if (!state.profile_events_block_out)
    {
        const Settings & query_settings = state.query_context->getSettingsRef();
        state.profile_events_block_out = std::make_unique<NativeWriter>(
            *out, client_tcp_protocol_version, block.cloneEmpty(), getFormatSettings(state.query_context), !query_settings.low_cardinality_allow_in_native_format);
    }
}

void TCPHandler::checkIfQueryCanceled(QueryState & state)
{
    if (state.stop_query)
        throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Packet 'Cancel' has been received from the client, canceling the query.");
}

void TCPHandler::processCancel(QueryState & state)
{
    if (state.allow_partial_result_on_first_cancel && !state.stop_read_return_partial_result)
    {
        state.stop_read_return_partial_result = true;
        LOG_INFO(log, "Received 'Cancel' packet from the client, returning partial result.");
            return;
    }

    state.read_all_data = true;
    state.stop_query = true;

    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT, "Received 'Cancel' packet from the client, canceling the query.");
}

void TCPHandler::receivePacketsExpectCancel(QueryState & state)
{
    if (after_check_cancelled.elapsed() / 1000 < interactive_delay)
        return;

    after_check_cancelled.restart();

    /// During request execution the only packet that can come from the client is stopping the query.
    if (in->poll(0))
    {
        if (in->isCanceled() || in->eof())
            throw NetException(ErrorCodes::ABORTED, "Client has dropped the connection, cancel the query.");

        UInt64 packet_type = 0;
        readVarUInt(packet_type, *in);

        switch (packet_type)
        {
            case Protocol::Client::Cancel:
                processCancel(state);
                break;

            default:
                throw NetException(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown packet from client {}", toString(packet_type));
        }
    }
}

void TCPHandler::sendData(QueryState & state, const Block & block)
{
    initBlockOutput(state, block);

    size_t prev_bytes_written_out = out->count();
    size_t prev_bytes_written_compressed_out = state.maybe_compressed_out->count();

    try
    {
        /// For testing hedged requests
        if (unknown_packet_in_send_data)
        {
            constexpr UInt64 marker = (1ULL << 63) - 1;
            --unknown_packet_in_send_data;
            if (unknown_packet_in_send_data == 0)
                writeVarUInt(marker, *out);
        }

        writeVarUInt(Protocol::Server::Data, *out);
        /// Send external table name (empty name is the main table)
        writeStringBinary("", *out);

        /// For testing hedged requests
        if (block.rows() > 0 && state.query_context->getSettingsRef().sleep_in_send_data_ms.totalMilliseconds())
        {
            out->next();
            std::chrono::milliseconds ms(state.query_context->getSettingsRef().sleep_in_send_data_ms.totalMilliseconds());
            std::this_thread::sleep_for(ms);
        }

        state.block_out->write(block);

        if (state.maybe_compressed_out != out)
            state.maybe_compressed_out->next();

        out->next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// In case of unsuccessful write, if the buffer with written data was not flushed,
        ///  we will rollback write to avoid breaking the protocol.
        /// (otherwise the client will not be able to receive exception after unfinished data
        ///  as it will expect the continuation of the data).
        /// It looks like hangs on client side or a message like "Data compressed with different methods".

        if (state.compression == Protocol::Compression::Enable)
        {
            auto extra_bytes_written_compressed = state.maybe_compressed_out->count() - prev_bytes_written_compressed_out;
            if (state.maybe_compressed_out->offset() >= extra_bytes_written_compressed)
                state.maybe_compressed_out->position() -= extra_bytes_written_compressed;
        }

        auto extra_bytes_written_out = out->count() - prev_bytes_written_out;
        if (out->offset() >= extra_bytes_written_out)
            out->position() -= extra_bytes_written_out;

        throw;
    }
}


void TCPHandler::sendLogData(QueryState & state, const Block & block)
{
    initLogsBlockOutput(state, block);

    if (out->isCanceled())
        return;

    writeVarUInt(Protocol::Server::Log, *out);
    /// Send log tag (empty tag is the default tag)
    writeStringBinary("", *out);

    state.logs_block_out->write(block);
    state.logs_block_out->flush();
    out->next();
}


void TCPHandler::sendTableColumns(QueryState &, const ColumnsDescription & columns)
{
    writeVarUInt(Protocol::Server::TableColumns, *out);

    /// Send external table name (empty name is the main table)
    writeStringBinary("", *out);
    writeStringBinary(columns.toString(), *out);

    out->next();
}


void TCPHandler::sendException(const Exception & e, bool with_stack_trace)
{
    if (out->isCanceled())
        return;

    writeVarUInt(Protocol::Server::Exception, *out);
    writeException(e, *out, with_stack_trace);

    out->next();
}


void TCPHandler::sendEndOfStream(QueryState & state)
{
    state.sent_all_data = true;
    writeVarUInt(Protocol::Server::EndOfStream, *out);
    out->next();
}


void TCPHandler::updateProgress(QueryState & state, const Progress & value)
{
    state.progress.incrementPiecewiseAtomically(value);
}


void TCPHandler::sendProgress(QueryState & state)
{
    writeVarUInt(Protocol::Server::Progress, *out);
    auto increment = state.progress.fetchValuesAndResetPiecewiseAtomically();
    UInt64 current_elapsed_ns = state.watch.elapsedNanoseconds();
    increment.elapsed_ns = current_elapsed_ns - state.prev_elapsed_ns;
    state.prev_elapsed_ns = current_elapsed_ns;
    increment.write(*out, client_tcp_protocol_version);

    out->next();
}


void TCPHandler::sendLogs(QueryState & state)
{
    if (!state.logs_queue)
        return;

    MutableColumns logs_columns;
    MutableColumns curr_logs_columns;
    size_t rows = 0;

    for (; state.logs_queue->tryPop(curr_logs_columns); ++rows)
    {
        if (rows == 0)
        {
            logs_columns = std::move(curr_logs_columns);
        }
        else
        {
            for (size_t j = 0; j < logs_columns.size(); ++j)
                logs_columns[j]->insertRangeFrom(*curr_logs_columns[j], 0, curr_logs_columns[j]->size());
        }
    }

    if (rows > 0)
    {
        Block block = InternalTextLogsQueue::getSampleBlock();
        block.setColumns(std::move(logs_columns));
        sendLogData(state, block);
    }
}


void TCPHandler::run()
{
    try
    {
        runImpl();
        LOG_DEBUG(log, "Done processing connection.");
    }
    catch (...)
    {
        tryLogCurrentException(log, "TCPHandler");
        throw;
    }
}

}
