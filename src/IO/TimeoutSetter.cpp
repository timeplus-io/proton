#include <IO/TimeoutSetter.h>

#include <Common/logger_useful.h>


namespace DB
{

TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_,
    Poco::Timespan send_timeout_,
    Poco::Timespan receive_timeout_,
    bool limit_max_timeout)
    : socket(socket_), send_timeout(send_timeout_), receive_timeout(receive_timeout_)
{
    old_send_timeout = socket.getSendTimeout();
    old_receive_timeout = socket.getReceiveTimeout();

    if (!limit_max_timeout || old_send_timeout > send_timeout)
        socket.setSendTimeout(send_timeout);

    if (!limit_max_timeout || old_receive_timeout > receive_timeout)
        socket.setReceiveTimeout(receive_timeout);
}

TimeoutSetter::TimeoutSetter(Poco::Net::StreamSocket & socket_, Poco::Timespan timeout_, bool limit_max_timeout)
    : TimeoutSetter(socket_, timeout_, timeout_, limit_max_timeout)
{
}

TimeoutSetter::~TimeoutSetter()
{
    if (was_reset)
        return;

    try
    {
        reset();
    }
    catch (...)
    {
        tryLogCurrentException("Client", "TimeoutSetter: Can't reset timeouts");
    }
}

void TimeoutSetter::reset()
{
    /// Poco sockets can be moved-from (impl() becomes null). Also, when the socket is already
    /// disconnected, there is no need to restore timeouts.
    auto * impl = socket.impl();
    if (!impl)
        return;

    bool connected = impl->initialized();
    if (!connected)
        return;

    socket.setSendTimeout(old_send_timeout);
    socket.setReceiveTimeout(old_receive_timeout);
    was_reset = true;
}

}
