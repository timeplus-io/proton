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

#if USE_SSL
    bool ssl_enabled = true;
#else
    bool ssl_enabled = false;
#endif

    std::atomic<Int32> last_connection_id = 0;
    std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> auth_methods;

public:
    explicit PostgreSQLHandlerFactory(IServer & server_);

    std::shared_ptr<Poco::Net::TCPServerConnection> createConnection(const Poco::Net::StreamSocket & socket, TCPServer & server) override;
};
}
