#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <base/StringRef.h>

namespace DB
{

class WriteBufferFromKafkaSink final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromKafkaSink(std::function<void(char * pos, size_t len)> on_next_, size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size)
    , on_next(on_next_)
    {
        assert(on_next);
    }

    ~WriteBufferFromKafkaSink() override = default;

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        on_next(working_buffer.begin(), offset());
    }

    std::function<void(char *, size_t)> on_next;
};

}
