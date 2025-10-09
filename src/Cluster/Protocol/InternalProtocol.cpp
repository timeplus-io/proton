#include <Cluster/Protocol/InternalProtocol.h>

#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED;
}

namespace cluster::protocol
{
uint16_t requestHeaderVersion(protocol::OpCode request_opcode, uint16_t /*request_version*/)
{
    switch (request_opcode)
    {
        /// TODO, fill the mapping
        default:
            return 1;
    }
}

bool isRequestVersionSupported(protocol::OpCode /*request_opcode*/, uint16_t /*request_version*/)
{
    /// TODO, fill the checking
    return true;
}

void checkRequestVersionSupported(protocol::OpCode request_opcode, uint16_t request_version)
{
    if (!isRequestVersionSupported(request_opcode, request_version))
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "");
}

uint16_t responseHeaderVersion(protocol::OpCode request_opcode, uint16_t /*request_version*/)
{
    switch (request_opcode)
    {
        /// TODO, fill the mapping
        default:
            return 1;
    }
}

}
