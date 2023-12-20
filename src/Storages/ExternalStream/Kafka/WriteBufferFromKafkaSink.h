#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <base/StringRef.h>

namespace DB
{

class WriteBufferFromKafkaSink final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromKafkaSink(const std::function<void(char * pos, size_t len)> callback, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size)
    , on_next(callback)
    {}

    ~WriteBufferFromKafkaSink() override = default;

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        on_next(working_buffer.begin(), offset());
    }

    const std::function<void(char *, size_t)> on_next;
};
}
