#pragma once

#include <cassert>
#include <cstdlib>
#include <memory>

#include <boost/core/noncopyable.hpp>


namespace nlog
{
class ByteVectorSlice final : public boost::noncopyable
{
public:
    using value_type = char;

    ByteVectorSlice(value_type * mem_, size_t siz_) : mem(mem_), siz(siz_) { }

    void resize(size_t new_size)
    {
        if (new_size > siz)
            throw std::logic_error("Can't resize ByteVectorSlice");
    }

    bool empty() const { return siz == 0; }

    const char * data() const { return mem; }

    char * data() { return mem; }

    size_t capacity() const { return siz; }

    size_t size() const { return siz; }

private:
    value_type * mem;
    size_t siz;
};

/// A simple byte vector implementation by using `malloc` instead of `new` since we like it
/// to inter-operate across C/C++
/// FIXME: make the allocator customizable
class ByteVector final : public boost::noncopyable
{
public:
    using value_type = char;

    explicit ByteVector(size_t reserved) : mem(nullptr, std::free), cap(reserved), siz(0)
    {
        mem.reset(static_cast<char *>(std::malloc(reserved)));
        assert(mem);
    }

    ByteVector(ByteVector && other) noexcept : mem(std::move(other.mem)), cap(other.cap), siz(other.siz) { }

    ByteVector(): mem(nullptr, std::free), cap(0), siz(0) { }

    ~ByteVector() = default;

    bool empty() const { return siz == 0; }

    /// Like vector, after resize, the size shall be updated
    void resize(size_t new_size)
    {
        if (cap >= new_size)
        {
            siz = new_size;
            return;
        }

        /// Don't need apply the multiplier here since the WriterBufferFromVector already did this
        MemPtr new_mem{static_cast<char *>(std::malloc(new_size)), std::free};
        assert(new_mem);

        std::memcpy(new_mem.get(), mem.get(), siz);
        mem.swap(new_mem);
        siz = new_size;
        cap = new_size;
    }

    char operator[](size_t n)
    {
        assert(n < siz);
        return *(mem.get() + n);
    }

    const char * data() const { return mem.get(); }

    char * data() { return mem.get(); }

    size_t capacity() const { return cap; }

    size_t size() const { return siz; }

    /// Release the ownership of the underlying memory
    /// Caller is responsible to deallocate the released memory after this call
    char * release()
    {
        siz = 0;
        return mem.release();
    }

    ByteVectorSlice slice(size_t start_pos, size_t length)
    {
        assert(start_pos < siz && start_pos + length < siz);

        return ByteVectorSlice(mem.get() + start_pos, length);
    }

    void swap(ByteVector & rhs)
    {
        mem.swap(rhs.mem);
        std::swap(cap, rhs.cap);
        std::swap(siz, rhs.siz);
    }

    std::string_view stringView() const
    {
        return std::string_view(mem.get(), siz);
    }

private:
    using MemPtr = std::unique_ptr<char, void (*)(void *)>;

private:
    MemPtr mem;
    size_t cap;
    size_t siz;
};
}
