#pragma once

#include <Client/Connection.h>
#include <Client/ConnectionParameters.h>

namespace DB
{

/// LibClient is for using as a library client without all the complexities for handling terminal stuff like ClientBase does.
/// This is not thread-safe.
class LibClient final
{
public:

    LibClient(ConnectionParameters params_, Poco::Logger * logger_);

    /// Sends the query to the server.
    void executeQuery(const String & query, const String & query_id = "");
    /// Cancels the currently running query, does nothing if there is no queries running.
    void cancelQuery();
    /// Polls data for a query previously sent with `executeQuery`. When no more data are available, the returned optional will be empty.
    std::optional<Block> pollData();

    void throwServerExceptionIfAny();

private:
    bool receiveAndProcessPacket();
    void reset();

    ConnectionParameters params;
    std::unique_ptr<Connection> connection;
    size_t poll_interval;

    std::atomic_bool cancelled {false};
    size_t processed_rows {0};
    Block next_data;
    std::unique_ptr<Exception> server_exception {nullptr};

    Poco::Logger * logger;
};

}
