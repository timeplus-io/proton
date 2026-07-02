#pragma once

#include <atomic>
#include <memory>
#include <Server/IServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Core/PostgreSQLProtocol.h>
#include "config.h"

namespace DB
{

class PostgreSQLHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    LoggerPtr log;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    bool ssl_enabled = false;

    std::atomic<Int32> last_connection_id = 0;
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> auth_methods;

public:
    explicit PostgreSQLHandlerFactory(
        IServer & server_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    std::shared_ptr<Poco::Net::TCPServerConnection> createConnection(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}
