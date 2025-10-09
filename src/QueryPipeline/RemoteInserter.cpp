#include <QueryPipeline/RemoteInserter.h>

#include <Client/Connection.h>
#include <Common/logger_useful.h>

#include <Common/NetException.h>
#include <Common/CurrentThread.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}


RemoteInserter::RemoteInserter(
    Connection & connection_,
    const ConnectionTimeouts & timeouts,
    const String & query_,
    const Settings & settings_,
    const ClientInfo & client_info_)
    : connection(connection_), query(query_)
{
    ClientInfo modified_client_info = client_info_;
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    /** Send query and receive "header", that describes stream structure.
      * Header is needed to know, what structure is required for blocks to be passed to 'write' method.
      */
    connection.sendQuery(
        timeouts, query, /* query_parameters */ {}, "", QueryProcessingStage::Complete, &settings_, &modified_client_info, false, {});

    while (true)
    {
        Packet packet = connection.receivePacket();

        if (Protocol::Server::Data == packet.type)
        {
            header = packet.block;
            break;
        }
        else if (Protocol::Server::Exception == packet.type)
        {
            packet.exception->rethrow();
            break;
        }
        else if (Protocol::Server::Log == packet.type)
        {
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
        }
        else if (Protocol::Server::ProfileEvents == packet.type)
        {
            // Do nothing
        }
        else if (Protocol::Server::TableColumns == packet.type)
        {
            /// Server could attach ColumnsDescription in front of stream for column defaults. There's no need to pass it through cause
            /// client's already got this information for remote table. Ignore.
        }
        else
            throw NetException(
                ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Unexpected packet from server (expected Data or Exception, got {})",
                Protocol::Server::toString(packet.type));
    }
}


void RemoteInserter::write(Block block)
{
    try
    {
        connection.sendData(block, /* name */"", /* scalar */false);
    }
    catch (const NetException &)
    {
        /// Try to get more detailed exception from server
        auto packet_type = connection.checkPacket(/* timeout_microseconds */0);
        if (packet_type && *packet_type == Protocol::Server::Exception)
        {
            Packet packet = connection.receivePacket();
            packet.exception->addMessage("Received exception from remote server"); /// proton: added
            packet.exception->rethrow();
        }

        throw;
    }
    /// proton: starts
    /// Remote server could send logs and profile events
    receiveLogsAndProfileEvents();
    /// proton: ends
}


void RemoteInserter::writePrepared(ReadBuffer & buf, size_t size)
{
    /// We cannot use 'header'. Input must contain block with proper structure.
    connection.sendPreparedData(buf, size);
}


void RemoteInserter::onFinish()
{
    /// Empty block means end of data.
    connection.sendData(Block(), /* name */"", /* scalar */false);

    /// Wait for EndOfStream or Exception packet, skip Log packets.
    while (true)
    {
        Packet packet = connection.receivePacket();

        if (Protocol::Server::EndOfStream == packet.type)
            break;
        else if (Protocol::Server::Exception == packet.type)
            packet.exception->rethrow();
        else if (Protocol::Server::Log == packet.type)
        {
            // Do nothing
        }
        else if (Protocol::Server::ProfileEvents == packet.type)
        {
            // Do nothing
        }
        else
            throw NetException(
                ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Unexpected packet from server (expected EndOfStream or Exception, got {})",
                Protocol::Server::toString(packet.type));
    }

    finished = true;
}

RemoteInserter::~RemoteInserter()
{
    /// If interrupted in the middle of the loop of communication with the server, then interrupt the connection,
    ///  to not leave the connection in unsynchronized state.
    if (!finished)
    {
        try
        {
            connection.disconnect();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

/// proton: starts
void RemoteInserter::receiveLogsAndProfileEvents()
{
    LoggerPtr logger = getLogger("RemoteInserter");
    auto packet_type = connection.checkPacket(0);

    while (packet_type)
    {
        Packet packet = connection.receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Log:
                if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                    log_queue->pushBlock(std::move(packet.block));
                break;

            case Protocol::Server::ProfileEvents:
                if (auto profile_events_queue = CurrentThread::getInternalProfileEventsQueue())
                    if (!profile_events_queue->push(std::move(packet.block)))
                        LOG_WARNING(logger, "Got a ProfileEvents packet from remote server, but failed to push it to the internal profile events queue");
                break;

            case Protocol::Server::Exception:
                packet.exception->addMessage("Received exception from remote server");
                packet.exception->rethrow();
                break;

            default:
                throw NetException(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server (expected Exception, Log or ProfileEvents. Got {})",
                    Protocol::Server::toString(packet.type));
        }

        packet_type = connection.checkPacket(0);
    }
}
/// proton: ends

}
