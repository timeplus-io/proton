#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <base/StringRef.h>

namespace DB
{

class WriteBufferFromKafkaSink final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromKafkaSink(
        std::function<void(char * pos, size_t len, size_t total_len)> on_next_,
        std::function<void()> after_next_,
        size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
    : BufferWithOwnMemory<WriteBuffer>(buffer_size)
    , on_next(std::move(on_next_))
    , after_next(std::move(after_next_))
    {
        assert(on_next);
    }

    ~WriteBufferFromKafkaSink() override = default;

    void markOffset() noexcept { marked_offset = offset(); }

private:
    void nextImpl() override
    {
        if (!offset())
            return;

        on_next(working_buffer.begin(), marked_offset, offset());
    }

    void afterNext() override
    {
        after_next();
        marked_offset = 0;
    }

    /// Callback for handling the next chunk of data.
    /// \param pos the begin position of the data.
    /// \param len the size of the data which contain the complete rows.
    /// \param total_len the size of the whole available data, the data availble at between `len` and `total_len` is in-complete data, which means the buffer doesnot have enough space for storing that row.
    std::function<void(char * pos, size_t len, size_t total_len)> on_next;

    std::function<void()> after_next;

    size_t marked_offset = 0;
};

}
