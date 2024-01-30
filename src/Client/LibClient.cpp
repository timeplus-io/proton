#include <Client/ConnectionParameters.h>
#include <Client/LibClient.h>

namespace DB
{

namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
extern const int TIMEOUT_EXCEEDED;
extern const int UNKNOWN_PACKET_FROM_SERVER;
}

namespace
{

std::unique_ptr<Connection> createConnection(const ConnectionParameters & parameters)
{
    return std::make_unique<Connection>(
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
}

size_t calculatePollInterval(const ConnectionTimeouts & timeouts)
{
    const auto & receive_timeout = timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1'000'000; /// in microseconds
    constexpr size_t min_poll_interval = 5'000; /// in microseconds
    return std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));
}

}

LibClient::LibClient(ConnectionParameters params_, Poco::Logger * logger_)
    : params(params_)
    , connection(createConnection(params))
    , poll_interval(calculatePollInterval(params.timeouts))
    , logger(logger_)
{
}

void LibClient::reset()
{
    cancelled = false;
    processed_rows = 0;
    server_exception = nullptr;
}

void LibClient::executeQuery(const String & query, const String & query_id)
{
    reset();

    int retries_left = 10;
    while (retries_left)
    {
        try
        {
            connection->sendQuery(
                params.timeouts,
                query,
                {},
                query_id,
                QueryProcessingStage::Complete,
                nullptr,
                nullptr,
                false);

            break;
        }
        catch (const Exception & e)
        {
            /// Retry when the server said "Client should retry" and no rows
            /// has been received yet.
            if (processed_rows == 0 && e.code() == ErrorCodes::DEADLOCK_AVOIDED && --retries_left)
                LOG_ERROR(logger, "Got a transient error from the server, will retry ({} retries left)", retries_left);
            else
                throw;
        }
    }
}

std::optional<Block> LibClient::pollData()
{
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
            return std::nullopt;

        return std::move(next_data);
    }
}

void LibClient::cancelQuery()
{
    LOG_INFO(logger, "Query was cancelled.");
    connection->sendCancel();
    cancelled = true;
}

/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
bool LibClient::receiveAndProcessPacket()
{
    Packet packet = connection->receivePacket();

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            return true;

        case Protocol::Server::Data:
            next_data = std::move(packet.block);
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

        case Protocol::Server::Exception:
            server_exception.swap(packet.exception);
            return false;

        case Protocol::Server::Log:
            /// on_server_log(packet.block);
            return true;

        case Protocol::Server::EndOfStream:
            return false;

        case Protocol::Server::ProfileEvents:
            /// on_profile_event(packet.block);
            return true;

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
    }
}

void LibClient::throwServerExceptionIfAny()
{
    if (server_exception)
        server_exception->rethrow();
}

}
