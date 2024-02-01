#pragma once

#include <Client/Connection.h>
#include <Client/ConnectionParameters.h>

namespace DB
{

/// ClickHouseClient is a client that is compatiable with the ClickHouse protocol and can be used to talk to ClickHouse servers.
/// Note:
///   * This client is designed for the ClickHouse ExternalTable, so it's not 100% compatiable with ClickHouse protocol, it just needs to make sure the ExternalTable is functional.
///   * A client object should not be shared with multiple threads.
class ClickHouseClient final
{
public:

    ClickHouseClient(ConnectionParameters params_, Poco::Logger * logger_);

    /// Sends the query to the server to execute. For insert queries, use `executeInsertQuery` instead.
    /// Make sure keep calling the `pollData` method until it returns an empty optional, until which the
    /// client won't be able to execute another query.
    void executeQuery(const String & query, const String & query_id = "");
    /// Sends an insert query to the server to execute. The difference between this and executeQuery is that,
    /// after calling this method, there is no need to call the `pollData` method.
    void executeInsertQuery(const String & query, const String & query_id = "");
    /// Cancels the currently running query, does nothing if there is no queries running.
    void cancelQuery();
    /// Polls data for a query previously sent with `executeQuery`. When no more data are available,
    /// the returned optional will be empty.
    std::optional<Block> pollData();
    /// Throw the server exception received from the ClickHouse server if any (during `pollData` or `executeInsertQuery`).
    void throwServerExceptionIfAny();

private:
    bool receiveAndProcessPacket();
    bool receiveEndOfQuery();

    void reset();

    void onEndOfStream();
    void onServerException(std::unique_ptr<Exception> && exception);

    ConnectionParameters params;
    std::unique_ptr<Connection> connection;
    size_t poll_interval;

    bool has_running_query {false};
    bool cancelled {false};
    size_t processed_rows {0};
    Block polled_data;
    std::unique_ptr<Exception> server_exception {nullptr};

    Poco::Logger * logger;
};

}
