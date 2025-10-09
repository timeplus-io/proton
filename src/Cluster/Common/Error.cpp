#include <Cluster/Common/Error.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ErrorCodes.h>

#include <fmt/format.h>

namespace cluster
{
void Error::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeVarInt(error_code, wb);
    DB::writeStringBinary(error_message, wb);
}

void Error::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readVarInt(error_code, rb);
    DB::readStringBinary(error_message, rb);
}

std::string Error::string() const
{
    return fmt::format(
        "error_code={} error_message='{}'", error_code, error_message.empty() ? DB::ErrorCodes::getName(error_code) : error_message);
}
}
