#pragma once

#include <Cluster/Common/SerdeTag.h>
#include <Cluster/Common/utils.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

namespace ErrorCodes
{
extern const int OK;
extern const int RESOURCE_NOT_FOUND;
}
}

namespace cluster
{
SERDE struct Error
{
    Error() = default;

    explicit Error(int32_t error_code_) : error_code(error_code_) { }

    Error(int32_t error_code_, const std::string & error_message_) : error_code(error_code_), error_message(std::move(error_message_)) { }

    Error(int32_t error_code_, std::string && error_message_) : error_code(error_code_), error_message(std::move(error_message_)) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;

    void deserialize(DB::ReadBuffer & rb, uint16_t version);

    size_t approximateSerializedSize() const noexcept { return sizeof(error_code) + approximateSerializedSizeOf(error_message); }

    bool hasError() const noexcept { return error_code != DB::ErrorCodes::OK; }

    void swap(Error & other) noexcept
    {
        std::swap(error_code, other.error_code);
        std::swap(error_message, other.error_message);
    }

    std::string string() const;

    bool operator==(const Error & other) const noexcept { return error_code == other.error_code && error_message == other.error_message; }
    bool isNotFound() const noexcept { return error_code == DB::ErrorCodes::RESOURCE_NOT_FOUND; }

    int32_t error_code = DB::ErrorCodes::OK;
    std::string error_message;
};

}
