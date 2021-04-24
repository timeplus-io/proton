#pragma once

#include <cstdlib>
#include <cassert>
#include <memory>

#include <boost/core/noncopyable.hpp>


namespace DB
{
class ByteVector final : public boost::noncopyable
{
public:
    using value_type = uint8_t;

    explicit ByteVector(size_t reserved) : mem(nullptr, std::free), cap(reserved), siz(0)
    {
        mem.reset(static_cast<uint8_t *>(std::malloc(reserved)));
        assert(mem);
    }

    ByteVector(ByteVector && other) noexcept : mem(move(other.mem)), cap(other.cap), siz(other.siz) { }

    ~ByteVector() = default;

    bool empty() const { return siz == 0; }

    void resize(size_t new_size)
    {
        if (cap >= new_size)
        {
            siz = new_size;
            return;
        }

        auto new_cap= static_cast<size_t>(cap* 1.5);
        MemPtr new_mem{static_cast<uint8_t *>(std::malloc(new_cap)), std::free};
        assert(new_mem);

        std::memcpy(new_mem.get(), mem.get(), siz);
        mem.swap(new_mem);
        siz = new_size;
        cap= new_cap;
    }

    uint8_t * data() { return mem.get(); }

    size_t capacity() const { return cap; }

    size_t size() const { return siz; }

    /// release the ownership of the underlying memory
    uint8_t * release()
    {
        siz = 0;
        return mem.release();
    }

private:
    using MemPtr = std::unique_ptr<uint8_t, void (*)(void *)>;

private:
    MemPtr mem;
    size_t cap;
    size_t siz;
};
}
