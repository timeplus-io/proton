#include <ClickHouse/Client.h>
#include <Common/NetException.h>

#include <base/sleep.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
extern const int LOGICAL_ERROR;
extern const int NO_FREE_CONNECTION;
extern const int TIMEOUT_EXCEEDED;
extern const int UNKNOWN_PACKET_FROM_SERVER;
extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace ClickHouse
{

namespace
{

size_t calculatePollInterval(const ConnectionTimeouts & timeouts)
{
    const auto & receive_timeout = timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1'000'000; /// in microseconds
    constexpr size_t min_poll_interval = 5'000; /// in microseconds
    return std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));
}

}

Client::Client(TypeNameProviderPtr type_name_provider_, ConnectionPool::Entry connection_, ConnectionTimeouts timeouts_, LoggerPtr logger_)
    : type_name_provider(std::move(type_name_provider_))
    , connection(std::move(connection_))
    , timeouts(std::move(timeouts_))
    , poll_interval(calculatePollInterval(timeouts))
    , logger(std::move(logger_))
{
}

Client::Client(
    TypeNameProviderPtr type_name_provider_,
    ConnectionPoolPtr connection_pool_,
    UInt64 connection_pool_max_wait_ms,
    ConnectionTimeouts timeouts_,
    LoggerPtr logger_)
    : type_name_provider(std::move(type_name_provider_))
    , connection_pool(std::move(connection_pool_))
    , timeouts(std::move(timeouts_))
    , poll_interval(calculatePollInterval(timeouts))
    , logger(std::move(logger_))
{
    get_connection_settings.emplace();
    get_connection_settings->connection_pool_max_wait_ms = connection_pool_max_wait_ms;
}

void Client::reset()
{
    cancelled = false;
    processed_rows = 0;
    server_exception = nullptr;
}

void Client::executeQuery(const String & query, const String & query_id, bool fail_quick)
{
    prepareConnection();
    chassert(connection.has_value());

    assert(!has_running_query);
    has_running_query = true;

    reset();

    bool suppress_error_log{false};
    while (true)
    {
        try
        {
            (*connection)
                ->sendQuery(
                    timeouts,
                    query,
                    /*query_parameters=*/{},
                    query_id,
                    QueryProcessingStage::Complete,
                    /*settings=*/nullptr,
                    /*client_info=*/nullptr,
                    /*with_pending_data=*/false,
                    /*process_progress_callback=*/{});

            break;
        }
        catch (const Exception & e)
        {
            if (fail_quick)
                e.rethrow();

            /// connection lost
            if (!(*connection)->checkConnected())
            {
                if (!suppress_error_log)
                    LOG_ERROR(logger, "Connection lost");
                /// set the connection not connected so that sendQuery will reconnect
                (*connection)->disconnect();
                sleepForSeconds(2);
            }
            /// Retry when the server said "Client should retry" and no rows has been received yet.
            else if (processed_rows == 0 && e.code() == ErrorCodes::DEADLOCK_AVOIDED)
            {
                if (!suppress_error_log)
                    LOG_ERROR(logger, "Got a transient error from the server, will retry in 1 second");
                sleepForSeconds(1);
            }
            else
            {
                has_running_query = false;
                e.rethrow();
            }

            /// Otherwise, it will keep generating the same error log again and again until the connection is back.
            suppress_error_log = true;
        }
    }
}

void Client::receiveInsertQueryResponse()
{
    chassert(connection.has_value());

    while (true)
    {
        Packet packet = (*connection)->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Exception:
                onServerException(std::move(packet.exception));
                return;

            case Protocol::Server::TableColumns:
                [[fallthrough]];
            case Protocol::Server::Log:
                [[fallthrough]];
            case Protocol::Server::ProfileEvents:
                [[fallthrough]];
            case Protocol::Server::Progress:
                continue;

            case Protocol::Server::Data:
                return;

            default:
                throw NetException(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server for insert: {}",
                    String(Protocol::Server::toString(packet.type)));
        }
    }
}

void Client::executeInsertQuery(const String & query, const Block & block, const String & query_id)
{
    prepareConnection();

    executeQuery(query, query_id);
    receiveInsertQueryResponse();
    throwServerExceptionIfAny();

    (*connection)->sendData(block, type_name_provider->getColumnTypeNames(), "", false);
    // Send empty block as marker of end of data.
    (*connection)->sendData(Block(), "", false);

    // Wait for EOS.
    receiveEndOfQuery();
}

std::optional<Block> Client::pollData()
{
    if (!has_running_query)
        return std::nullopt;

    chassert(connection.has_value());

    while (true)
    {
        Stopwatch receive_watch(CLOCK_MONOTONIC_COARSE);

        while (true)
        {
            if (!cancelled)
            {
                double elapsed = receive_watch.elapsedSeconds();
                if (elapsed > timeouts.receive_timeout.totalSeconds())
                {
                    cancelQuery();

                    throw Exception(
                        ErrorCodes::TIMEOUT_EXCEEDED,
                        "Timeout exceeded while receiving data from server. Waited for {} seconds, timeout is {} seconds",
                        static_cast<size_t>(elapsed),
                        timeouts.receive_timeout.totalSeconds());
                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.

            if ((*connection)->poll(poll_interval))
                break;
        }

        if (!receiveAndProcessPacket())
        {
            has_running_query = false;
            return std::nullopt;
        }

        return std::move(polled_data);
    }
}

void Client::cancelQuery()
{
    if (!has_running_query)
        return;

    LOG_INFO(logger, "Query cancelled.");
    if (connection)
        (*connection)->sendCancel();
    cancelled = true;
    has_running_query = false;
}

/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
bool Client::receiveAndProcessPacket()
{
    assert(has_running_query);
    chassert(connection.has_value());

    Packet packet = (*connection)->receivePacket();

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            return true;

        case Protocol::Server::Data:
            processed_rows += packet.block.rows();
            polled_data = std::move(packet.block);
            return true;

        case Protocol::Server::Progress:
            // on_progress(packet.progress);
            return true;

        case Protocol::Server::ProfileInfo:
            // on_profile_info(packet.profile_info);
            return true;

        case Protocol::Server::Totals:
            // on_totals(packet.block);
            return true;

        case Protocol::Server::Extremes:
            // on_extremes(packet.block);
            return true;

        case Protocol::Server::EndOfStream:
            onEndOfStream();
            return true;

        case Protocol::Server::Exception:
            onServerException(std::move(packet.exception));
            return false;

        case Protocol::Server::Log:
            /// on_server_log(packet.block);
            return true;

        case Protocol::Server::ProfileEvents:
            /// on_profile_event(packet.block);
            return true;

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, (*connection)->getDescription());
    }
}

/// Process Log packets, exit when receive Exception or EndOfStream
bool Client::receiveEndOfQuery()
{
    chassert(connection.has_value());

    while (true)
    {
        Packet packet = (*connection)->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::EndOfStream:
                onEndOfStream();
                return true;

            case Protocol::Server::Exception:
                onServerException(std::move(packet.exception));
                return false;

            case Protocol::Server::Log:
                /// onLogData(packet.block);
                break;

            case Protocol::Server::Progress:
                /// onProgress(packet.progress);
                break;

            case Protocol::Server::ProfileEvents:
                /// onProfileEvents(packet.block);
                break;

            default:
                throw NetException(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server (expected Exception, EndOfStream, Log, Progress or ProfileEvents. Got {})",
                    String(Protocol::Server::toString(packet.type)));
        }
    }
}

void Client::onEndOfStream()
{
    has_running_query = false;
}

void Client::onServerException(std::unique_ptr<Exception> && exception)
{
    server_exception.swap(exception);
    has_running_query = false;
}

void Client::throwServerExceptionIfAny()
{
    if (server_exception)
        server_exception->rethrow();
}

void Client::releaseConnection()
{
    if (connection.has_value())
    {
        connection.reset();
        LOG_DEBUG(logger, "Connection released.");
    }
}

void Client::prepareConnection()
{
    if (connection)
        return;

    if (!connection_pool)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get connection as connection pool is not provided.");

    connection = connection_pool->get(timeouts, &get_connection_settings.value(), true);
    if (!connection)
        throw Exception(
            ErrorCodes::NO_FREE_CONNECTION,
            "No connection available, please try stop some running queries or use a bigger value for pooled_connections setting");

    LOG_DEBUG(logger, "New connection prepared.");
}
}

}
