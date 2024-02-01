#include <Common/NetException.h>
#include <Client/ClickHouseClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
extern const int TIMEOUT_EXCEEDED;
extern const int UNKNOWN_PACKET_FROM_SERVER;
extern const int UNEXPECTED_PACKET_FROM_SERVER;
}

namespace
{

std::unique_ptr<Connection> createConnection(const ConnectionParameters & parameters)
{
    auto ret = std::make_unique<Connection>(
        parameters.host,
        parameters.port,
        parameters.default_database,
        parameters.user,
        parameters.password,
        parameters.quota_key,
        "", /* cluster */
        "", /* cluster_secret */
        "TimeplusProton",
        parameters.compression,
        parameters.security);

    ret->setCompatibleWithClickHouse();
    return ret;
}

size_t calculatePollInterval(const ConnectionTimeouts & timeouts)
{
    const auto & receive_timeout = timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1'000'000; /// in microseconds
    constexpr size_t min_poll_interval = 5'000; /// in microseconds
    return std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));
}

}

ClickHouseClient::ClickHouseClient(ConnectionParameters params_, Poco::Logger * logger_)
    : params(params_)
    , connection(createConnection(params))
    , poll_interval(calculatePollInterval(params.timeouts))
    , logger(logger_)
{
}

void ClickHouseClient::reset()
{
    cancelled = false;
    processed_rows = 0;
    server_exception = nullptr;
}

void ClickHouseClient::executeQuery(const String & query, const String & query_id, bool fail_quick)
{
    assert(!has_running_query);
    has_running_query = true;

    reset();

    bool suppress_error_log {false};
    while (true)
    {
        try
        {
            connection->sendQuery(
                params.timeouts,
                query,
                {} /*query_parameters*/,
                query_id,
                QueryProcessingStage::Complete,
                nullptr /*settings*/,
                nullptr /*client_info*/,
                false);

            break;
        }
        catch (const Exception & e)
        {
            if (fail_quick)
                e.rethrow();

            /// connection lost
            if (!connection->checkConnected())
            {
                if (!suppress_error_log)
                    LOG_ERROR(logger, "Connection lost");
                /// set the connection not connected so that sendQuery will reconnect
                connection->disconnect();
                std::this_thread::sleep_for(std::chrono::seconds(2));
            }
            /// Retry when the server said "Client should retry" and no rows has been received yet.
            else if (processed_rows == 0 && e.code() == ErrorCodes::DEADLOCK_AVOIDED)
            {
                if (!suppress_error_log)
                    LOG_ERROR(logger, "Got a transient error from the server, will retry in 1 second");
                std::this_thread::sleep_for(std::chrono::seconds(1));
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

void ClickHouseClient::executeInsertQuery(const String & query, const String & query_id)
{
    executeQuery(query, query_id);
    receiveEndOfQuery();
}

std::optional<Block> ClickHouseClient::pollData()
{
    if (!has_running_query)
        return std::nullopt;

    while (true)
    {
        Stopwatch receive_watch(CLOCK_MONOTONIC_COARSE);

        while (true)
        {
            if (!cancelled)
            {
                double elapsed = receive_watch.elapsedSeconds();
                if (elapsed > params.timeouts.receive_timeout.totalSeconds())
                {
                    cancelQuery();

                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded while receiving data from server. Waited for {} seconds, timeout is {} seconds", static_cast<size_t>(elapsed), params.timeouts.receive_timeout.totalSeconds());

                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.

            if (connection->poll(poll_interval))
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

void ClickHouseClient::cancelQuery()
{
    if (!has_running_query)
        return;

    LOG_INFO(logger, "Query cancelled.");
    connection->sendCancel();
    cancelled = true;
    has_running_query = false;
}

/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
bool ClickHouseClient::receiveAndProcessPacket()
{
    assert(has_running_query);

    Packet packet = connection->receivePacket();

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
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
    }
}

/// Process Log packets, exit when receive Exception or EndOfStream
bool ClickHouseClient::receiveEndOfQuery()
{
    while (true)
    {
        Packet packet = connection->receivePacket();

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
                throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server (expected Exception, EndOfStream, Log, Progress or ProfileEvents. Got {})",
                    String(Protocol::Server::toString(packet.type)));
        }
    }
}

void ClickHouseClient::onEndOfStream()
{
    has_running_query = false;
}

void ClickHouseClient::onServerException(std::unique_ptr<Exception> && exception)
{
    server_exception.swap(exception);
    has_running_query = false;
}

void ClickHouseClient::throwServerExceptionIfAny()
{
    if (server_exception)
        server_exception->rethrow();
}

}
