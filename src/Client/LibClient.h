#pragma once

#include <Client/ConnectionPool.h>
#include <Interpreters/Context_fwd.h>
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
    std::function<void(std::unique_ptr<Exception> && e)>  on_receive_exception_from_server;
    std::function<void(const ProfileInfo & profile_info)>  on_profile_info;
    std::function<void()>  on_end_of_stream;
    std::function<void(Block & block)>  on_profile_events;
};

/// LibClient is for using as a library client without all the complexities for handling terminal stuff like ClientBase does.
class LibClient final
{
public:
    LibClient(ConnectionPtr connection_, ConnectionTimeouts timeouts_, ContextPtr & context_, Poco::Logger * logger_);

    void executeQuery(String query, const Callbacks & callbacks);
    void receiveResult(const Callbacks & callbacks);
    void cancelQuery();

private:
    bool receiveAndProcessPacket(bool cancelled_, const Callbacks & callbacks);

    ConnectionPtr connection;
    ConnectionTimeouts timeouts;

    std::atomic_bool cancelled {false};

    ContextPtr & context [[maybe_unused]];
    Poco::Logger * logger;
};

}
