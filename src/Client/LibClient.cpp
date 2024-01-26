#include <Client/LibClient.h>
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
extern const int TIMEOUT_EXCEEDED;
extern const int UNKNOWN_PACKET_FROM_SERVER;
}

LibClient::LibClient(ConnectionPtr connection_, ConnectionTimeouts timeouts_, ContextPtr & context_, Poco::Logger * logger_)
    : connection(connection_)
    , timeouts(timeouts_)
    , context(context_)
    , logger(logger_)
{}

void LibClient::executeQuery(String query, const Callbacks & callbacks)
{
    size_t processed_rows {0};
    int retries_left = 10;
    while (retries_left)
    {
        try
        {
            connection->sendQuery(
                timeouts,
                query,
                {},
                "",
                QueryProcessingStage::Complete,
                nullptr,
                nullptr,
                true);

            receiveResult(callbacks);

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

/// Receives and processes packets coming from server.
/// Also checks if query execution should be cancelled.
void LibClient::receiveResult(const Callbacks & callbacks)
{
    const auto receive_timeout = timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1000000; /// in microseconds
    constexpr size_t min_poll_interval = 5000; /// in microseconds
    const size_t poll_interval
        = std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));

    while (true)
    {
        Stopwatch receive_watch(CLOCK_MONOTONIC_COARSE);

        while (true)
        {
            /// Has the Ctrl+C been pressed and thus the query should be cancelled?
            /// If this is the case, inform the server about it and receive the remaining packets
            /// to avoid losing sync.
            if (!cancelled)
            {
                double elapsed = receive_watch.elapsedSeconds();
                if (elapsed > receive_timeout.totalSeconds())
                {
                    cancelQuery();

                    throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded while receiving data from server. Waited for {} seconds, timeout is {} seconds", static_cast<size_t>(elapsed), receive_timeout.totalSeconds());

                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.

            if (connection->poll(poll_interval))
                break;
        }

        if (!receiveAndProcessPacket(cancelled, callbacks))
            break;
    }

    if (cancelled)
        LOG_INFO(logger, "Query was cancelled.");
}

void LibClient::cancelQuery()
{
    connection->sendCancel();
    cancelled = true;
}

/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
/// Output of result is suppressed if query was cancelled.
bool LibClient::receiveAndProcessPacket(bool cancelled_, const Callbacks & callbacks)
{
    Packet packet = connection->receivePacket();

    Chunk chunk {};

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            return true;

        case Protocol::Server::Data:
            if (!cancelled_)
                callbacks.on_data(packet.block);
            return true;

        case Protocol::Server::Progress:
            if (callbacks.on_progress)
                callbacks.on_progress(packet.progress);
            return true;

        case Protocol::Server::ProfileInfo:
            if (callbacks.on_profile_info)
                callbacks.on_profile_info(packet.profile_info);
            return true;

        case Protocol::Server::Totals:
            if (!cancelled_)
                if (callbacks.on_totals)
                    callbacks.on_totals(packet.block);
            return true;

        case Protocol::Server::Extremes:
            if (!cancelled_)
                if (callbacks.on_extremes)
                    callbacks.on_extremes(packet.block);
            return true;

        case Protocol::Server::Exception:
            if (callbacks.on_receive_exception_from_server)
                callbacks.on_receive_exception_from_server(std::move(packet.exception));
            return false;

        case Protocol::Server::Log:
            if (callbacks.on_log_data)
                callbacks.on_log_data(packet.block);
            return true;

        case Protocol::Server::EndOfStream:
            if (callbacks.on_end_of_stream)
                callbacks.on_end_of_stream();
            return false;

        case Protocol::Server::ProfileEvents:
            if (callbacks.on_profile_events)
                callbacks.on_profile_events(packet.block);
            return true;

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
    }
}

// void LibClient::onProgress(const Progress & value)
// {
//     LOG_INFO(logger, "onProgress called with read_rows = {}", value.read_rows);
// }
//
// void LibClient::onData(Block & block)
// {
//     /// TBD
// }
//
// void LibClient::onLogData(Block & block) {
//     LOG_INFO(logger, "onLogData called with columns = {}, rows = {}", block.columns(), block.rows());
// }
//
// void LibClient::onTotals(Block & block)
// {
//     LOG_INFO(logger, "onTotals called with columns = {}, rows = {}", block.columns(), block.rows());
// }
//
// void LibClient::onExtremes(Block & block)
// {
//     LOG_INFO(logger, "onExtremes called with columns = {}, rows = {}", block.columns(), block.rows());
// }
//
// void LibClient::onReceiveExceptionFromServer(std::unique_ptr<Exception> && e)
// {
//     LOG_INFO(logger, "received server exception: {}", e->what());
// }
//
// void LibClient::onProfileInfo(const ProfileInfo & profile_info)
// {
//     LOG_INFO(logger, "received ProfileInfo: rows={}", profile_info.rows);
// }
// void LibClient::onEndOfStream()
// {
//     LOG_INFO(logger, "received EndOfStream");
// }
// void LibClient::onProfileEvents(Block & block)
// {
//     LOG_INFO(logger, "received ProfileEvents rows = {}", block.rows());
// }

}
