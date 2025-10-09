#pragma once

#include <Common/CurrentMetrics.h>

#include <cassert>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <boost/core/noncopyable.hpp>


namespace CurrentMetrics
{
extern const Metric ByteVectorReallocates;
}

namespace cluster
{
class ByteVectorSlice final
{
public:
    using value_type = char;

    ByteVectorSlice(value_type * mem_, size_t siz_) : mem(mem_), siz(siz_) { }

    [[noreturn]] void resize(size_t /*new_size*/) { throw std::logic_error("Can't resize ByteVectorSlice"); }

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
/// to inter-operate across C/C++ and the internal memory can be moved out
class ByteVector final
{
public:
    using value_type = char;

    ByteVector() : mem(nullptr, std::free), cap(0), siz(0) { }

    explicit ByteVector(size_t reserved) : mem(nullptr, std::free), cap(reserved), siz(reserved)
    {
        if (reserved)
            /// If `reserved` is 0, it is undefined if std::malloc will return an nullptr or a
            /// one byte memory addr. So only call malloc when `reserved` is not zero.
            mem.reset(static_cast<char *>(std::malloc(reserved)));
    }

    explicit ByteVector(std::string_view data) : ByteVector(data.size())
    {
        if (!data.empty())
            std::memcpy(mem.get(), data.data(), data.size());
    }

    ByteVector(ByteVector && other) noexcept : mem(std::move(other.mem)), cap(other.cap), siz(other.siz) { }

    ByteVector & operator=(ByteVector && other)
    {
        mem.swap(other.mem);
        cap = other.cap;
        siz = other.siz;

        return *this;
    }

    ~ByteVector() = default;

    bool empty() const { return siz == 0; }

    void reserve(size_t reserved)
    {
        if (cap)
            /// We don't actually reserve / reallocate if we already allocated
            return;

        assert(siz == 0 && !mem);
        mem.reset(static_cast<char *>(std::malloc(reserved)));
        cap = reserved;
        siz = reserved;
    }

    /// Like vector, after resize, the size shall be updated
    void resize(size_t new_size)
    {
        if (cap >= new_size)
        {
            /// std::cout << "cap = " << cap << " size= " << siz << " new_size=" << new_size << "\n";
            siz = new_size;
            return;
        }

        /// Don't need apply the multiplier here since we rely one caller like WriterBufferFromVector does this
        MemPtr new_mem{static_cast<char *>(std::malloc(new_size)), std::free};
        assert(new_mem);

        std::memcpy(new_mem.get(), mem.get(), siz);
        mem.swap(new_mem);
        siz = new_size;
        cap = new_size;

        CurrentMetrics::Increment metric_increment{CurrentMetrics::ByteVectorReallocates};
    }

    char operator[](size_t n) const
    {
        assert(n < siz);
        return *(mem.get() + n);
    }

    const char * data() const { return mem.get(); }

    char * data() { return mem.get(); }

    size_t capacity() const { return cap; }

    size_t size() const { return siz; }

    size_t approximateSerializedSize() const noexcept { return sizeof(siz) + siz; }

    /// Copy prefix_length to first 4 bytes of data buffer
    void prefixWithLength(uint32_t prefix_length)
    {
        assert(size() >= sizeof(prefix_length));
        std::memcpy(data(), &prefix_length, sizeof(prefix_length));
    }

    /// Copy prefix_flag to first 8 bytes of data buffer
    void prefixWithFlag(uint64_t prefix_flag)
    {
        assert(size() >= sizeof(prefix_flag));
        std::memcpy(data(), &prefix_flag, sizeof(prefix_flag));
    }

    /// Release the ownership of the underlying memory
    /// Caller is responsible to deallocate the released memory after this call
    char * release()
    {
        siz = 0;
        cap = 0;
        return mem.release();
    }

    ByteVectorSlice slice(size_t start_pos, size_t length)
    {
        assert(start_pos < siz && start_pos + length < siz);

        return ByteVectorSlice(mem.get() + start_pos, length);
    }

    void swap(ByteVector & rhs) noexcept
    {
        mem.swap(rhs.mem);
        std::swap(cap, rhs.cap);
        std::swap(siz, rhs.siz);
    }

    std::string_view stringView() const noexcept
    {
        if (!empty())
            return std::string_view(mem.get(), siz);
        else
            return std::string_view{};
    }

    std::string string() const
    {
        if (!empty())
            return std::string(mem.get(), siz);
        else
            return std::string{};
    }

    ByteVector clone() const { return ByteVector{stringView()}; }

    bool operator==(const ByteVector & other) const noexcept { return stringView() == other.stringView(); }

private:
    using MemPtr = std::unique_ptr<char, void (*)(void *)>;

private:
    MemPtr mem;
    size_t cap;
    size_t siz;
};
}
