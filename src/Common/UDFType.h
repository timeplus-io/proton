#pragma once

namespace DB
{
enum class UDFType: uint8_t
{
    Native,
    Javascript,
    Executable,
    Remote,
};
}
