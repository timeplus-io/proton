#pragma once

#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/TCPHandler.h>
#include <Server/TCPServerConnectionFactory.h>

namespace Poco { class Logger; }

namespace DB
{

class TCPHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    bool parse_proxy_protocol = false;
    /// proton: starts
    bool snapshot_mode = false;
    /// proton: ends
    LoggerPtr log;
    std::string server_display_name;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    /** parse_proxy_protocol_ - if true, expect and parse the header of PROXY protocol in every connection
      * and set the information about forwarded address accordingly.
      * See https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
      */
    TCPHandlerFactory(
        IServer & server_,
        bool secure_,
        bool parse_proxy_protocol_j,
        bool snapshot_mode_ = false,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
        : server(server_)
        , parse_proxy_protocol(parse_proxy_protocol_j)
        , snapshot_mode(snapshot_mode_)
        , log(getLogger(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        , read_event(read_event_)
        , write_event(write_event_)
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    std::shared_ptr<Poco::Net::TCPServerConnection> createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

            return std::make_shared<TCPHandler>(server, tcp_server, socket, parse_proxy_protocol, server_display_name, read_event, write_event, snapshot_mode);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return std::make_shared<DummyTCPHandler>(socket);
        }
    }
};

}
