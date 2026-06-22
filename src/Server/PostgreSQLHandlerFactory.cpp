#include "PostgreSQLHandlerFactory.h"
#include <Server/PostgreSQLHandler.h>
#include <Poco/Util/LayeredConfiguration.h>

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

#if USE_SSL
    /// Only enable SSL if certificate and private key files are actually configured and exist on disk.
    /// Without valid certs, defaultServerContext() will fail and the connection drops with EOF,
    /// even though the client already sent SSL_REQUEST. See: https://github.com/timeplus-io/proton/issues/824
    const auto & cfg = server.config();
    const std::string cert_file = cfg.getString("openSSL.server.certificateFile", "");
    const std::string key_file = cfg.getString("openSSL.server.privateKeyFile", "");
    if (!cert_file.empty() && !key_file.empty()
        && std::filesystem::exists(cert_file) && std::filesystem::exists(key_file))
    {
        ssl_enabled = true;
    }
    else
    {
        LOG_INFO(log, "SSL is disabled for PostgreSQL protocol: certificate or private key file not found "
                      "(certificateFile={}, privateKeyFile={})", cert_file, key_file);
    }
#endif
}

std::shared_ptr<Poco::Net::TCPServerConnection> PostgreSQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "PostgreSQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
    return std::make_shared<PostgreSQLHandler>(socket, server, tcp_server, ssl_enabled, connection_id, auth_methods);
}

}
