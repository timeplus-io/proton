#include "PostgreSQLHandlerFactory.h"
#include <memory>
#include <Server/PostgreSQLHandler.h>

namespace DB
{

PostgreSQLHandlerFactory::PostgreSQLHandlerFactory(IServer & server_)
    : server(server_)
    , log(getLogger("PostgreSQLHandlerFactory"))
{
    auth_methods =
    {
        std::make_shared<PostgreSQLProtocol::PGAuthentication::NoPasswordAuth>(),
        std::make_shared<PostgreSQLProtocol::PGAuthentication::CleartextPasswordAuth>(),
    };
}

std::shared_ptr<Poco::Net::TCPServerConnection> PostgreSQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "PostgreSQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
    return std::make_shared<PostgreSQLHandler>(socket, server, tcp_server, ssl_enabled, connection_id, auth_methods);
}

}
