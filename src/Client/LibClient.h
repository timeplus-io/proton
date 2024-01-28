#pragma once

#include <Client/ConnectionPool.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{

struct Callbacks
{
    std::function<void(const Progress & value)> on_progress;
    std::function<void(Block & block)>  on_data;
    std::function<void(Block & block)> on_log_data;
    std::function<void(Block & block)> on_totals;
    std::function<void(Block & block)>  on_extremes;
    std::function<void(Exception & e)>  on_receive_exception_from_server;
    std::function<void(const ProfileInfo & profile_info)>  on_profile_info;
    std::function<void()>  on_end_of_stream;
    std::function<void(Block & block)>  on_profile_events;
};

/// LibClient is for using as a library client without all the complexities for handling terminal stuff like ClientBase does.
class LibClient final
{
public:
    LibClient(Connection & connection_, ConnectionTimeouts timeouts_, Poco::Logger * logger_);

    void executeQuery(String query, const Callbacks & callbacks);
    void receiveResult(const Callbacks & callbacks);
    void cancelQuery();

    void throwServerExceptionIfAny();

private:
    bool receiveAndProcessPacket(bool cancelled_, const Callbacks & callbacks);

    Connection & connection;
    ConnectionTimeouts timeouts;

    std::atomic_bool cancelled {false};
    std::unique_ptr<Exception> server_exception {nullptr};

    Poco::Logger * logger;
};

}
