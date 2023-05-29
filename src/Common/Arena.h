#pragma once

#include <cstring>
#include <memory>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Core/Defines.h>
#if __has_include(<sanitizer/asan_interface.h>) && defined(ADDRESS_SANITIZER)
#   include <sanitizer/asan_interface.h>
#endif
#include <Common/memcpySmall.h>
#include <Common/ProfileEvents.h>
#include <Common/Allocator.h>

/// proton: starts
#include <base/ClockUtils.h>
#include <Common/BitHelpers.h>
/// proton: ends

namespace ProfileEvents
{
    extern const Event ArenaAllocChunks;
    extern const Event ArenaAllocBytes;
}

namespace DB
{

/** Memory pool to append something. For example, short strings.
  * Usage scenario:
  * - put lot of strings inside pool, keep their addresses;
  * - addresses remain valid during lifetime of pool;
  * - at destruction of pool, all memory is freed;
  * - memory is allocated and freed by large MemoryChunks;
  * - freeing parts of data is not possible (but look at ArenaWithFreeLists if you need);
  */
class Arena : private boost::noncopyable
{
private:
    /// Padding allows to use 'memcpySmallAllowReadWriteOverflow15' instead of 'memcpy'.
    static constexpr size_t pad_right = 15;

    /// Contiguous MemoryChunk of memory and pointer to free space inside it. Member of single-linked list.
    struct alignas(16) MemoryChunk : private Allocator<false>    /// empty base optimization
    {
        char * begin;
        char * pos;
        char * end; /// does not include padding.

        MemoryChunk * prev;

        /// proton: starts. Make arena time-aware for streaming processing to allow
        /// free MemoryChunk according to timestamp
        Int64 timestamp = std::numeric_limits<Int64>::min();
        /// proton: ends

        MemoryChunk(size_t size_, MemoryChunk * prev_, Int64 timestamp_)
        {
            ProfileEvents::increment(ProfileEvents::ArenaAllocChunks);
            ProfileEvents::increment(ProfileEvents::ArenaAllocBytes, size_);

            begin = reinterpret_cast<char *>(Allocator<false>::alloc(size_));
            pos = begin;
            end = begin + size_ - pad_right;
            prev = prev_;

            /// proton: starts
            timestamp = timestamp_;
            /// proton: ends

            ASAN_POISON_MEMORY_REGION(begin, size_);
        }

        ~MemoryChunk()
        {
            /// We must unpoison the memory before returning to the allocator,
            /// because the allocator might not have asan integration, and the
            /// memory would stay poisoned forever. If the allocator supports
            /// asan, it will correctly poison the memory by itself.
            ASAN_UNPOISON_MEMORY_REGION(begin, size());

            Allocator<false>::free(begin, size());

            if (prev)
                delete prev;
        }

        size_t size() const { return end + pad_right - begin; }
        size_t remaining() const { return end - pos; }
    };

    size_t growth_factor;
    size_t linear_growth_threshold;

    /// Last contiguous MemoryChunk of memory.
    MemoryChunk * head;
    size_t size_in_bytes;
    size_t page_size;

    /// proton: starts. Make arena time-aware for streaming processing to allow
    /// free MemoryChunk according to timestamp
    /// How to enable memory recycling :
    /// 1) Just right after ctor Arena and before any memory allocation from it, call arena.enableRecycle()
    /// 2) For every memory allocation from arena, first call arena.setCurrentTimestamp(timestamp),
    ///    then the current Chunk which is used to allocate the memory will be tagged with a timestamp.
    ///    It is basically saying the `Chunk` contains data before the timestamp
    /// 3) As timestamp progress, if clients don't need any data before `t`, call `arena.free(t)` which will walk through the chunk list
    ///    and recycle these which have upper bound timestamp less than `t`.
    /// 4) Depending on size etc, some of the recycled chunks will be added to `free_lists` for future memory allocation
    Int64 current_timestamp = std::numeric_limits<Int64>::min();

    size_t chunks = 0;
    size_t size_in_bytes_in_free_lists = 0;
    size_t chunks_in_free_lists = 0;
    size_t free_list_hits = 0;
    size_t free_list_misses = 0;

    struct MemoryChunkWrapper
    {
        uint64_t time_in_free_list = 0;
        MemoryChunk * chunk = nullptr;
    };

    std::array<MemoryChunkWrapper, 21> free_lists{};
    bool recycle_enabled = false;
    bool last_free_list_hit = false;
    /// proton: ends

    static size_t roundUpToPageSize(size_t s, size_t page_size)
    {
        return (s + page_size - 1) / page_size * page_size;
    }

    /// If MemoryChunks size is less than 'linear_growth_threshold', then use exponential growth, otherwise - linear growth
    ///  (to not allocate too much excessive memory).
    size_t nextSize(size_t min_next_size) const
    {
        size_t size_after_grow = 0;

        if (head->size() < linear_growth_threshold)
        {
            size_after_grow = std::max(min_next_size, head->size() * growth_factor);
        }
        else
        {
            // allocContinue() combined with linear growth results in quadratic
            // behavior: we append the data by small amounts, and when it
            // doesn't fit, we create a new MemoryChunk and copy all the previous data
            // into it. The number of times we do this is directly proportional
            // to the total size of data that is going to be serialized. To make
            // the copying happen less often, round the next size up to the
            // linear_growth_threshold.
            size_after_grow = ((min_next_size + linear_growth_threshold - 1)
                    / linear_growth_threshold) * linear_growth_threshold;
        }

        assert(size_after_grow >= min_next_size);
        return roundUpToPageSize(size_after_grow, page_size);
    }

    /// proton: starts
    /// Add next contiguous MemoryChunk of memory with size not less than specified.
    void NO_INLINE addMemoryChunk(size_t min_size)
    {
        if (recycle_enabled)
        {
            auto success = allocateFromFreeLists(min_size + pad_right);
            if (success)
                return;
        }

        head = new MemoryChunk(nextSize(min_size + pad_right), head, current_timestamp);
        size_in_bytes += head->size();

        /// proton: starts
        ++chunks;
        /// proton: ends
    }

    bool allocateFromFreeLists(size_t size)
    {
        assert(recycle_enabled);

        /// If last_free_list_hit is true which means the `head` shrinks
        /// we cycle through the free list by bumping the size
        if (last_free_list_hit)
            size = nextSize(size);
        else
            size = roundUpToPageSize(size, page_size);

        auto index = bitScanReverse(size / page_size);
        if (likely(index < free_lists.size()))
        {
            auto * chunk = free_lists[index].chunk;
            if (chunk)
            {
                chunk->timestamp = current_timestamp;

                free_lists[index].chunk = chunk->prev;
                /// Since we have only one chunk in each free list slot
                assert(free_lists[index].chunk == nullptr);

                chunk->prev = head;
                head = chunk;

                free_lists[index].time_in_free_list = 0;
                size_in_bytes_in_free_lists -= chunk->size();
                --chunks_in_free_lists;

                ++chunks;
                size_in_bytes += chunk->size();

                ++free_list_hits;
                last_free_list_hit = true;
                return true;
            }
        }
        ++free_list_misses;
        last_free_list_hit = false;
        return false;
    }
    /// proton: ends

    friend class ArenaAllocator;
    template <size_t> friend class AlignedArenaAllocator;

public:
    explicit Arena(size_t initial_size_ = 4096, size_t growth_factor_ = 2, size_t linear_growth_threshold_ = 128 * 1024 * 1024)
        : growth_factor(growth_factor_), linear_growth_threshold(linear_growth_threshold_),
        head(new MemoryChunk(initial_size_, nullptr, std::numeric_limits<Int64>::min())), size_in_bytes(head->size()),
        page_size(static_cast<size_t>(::getPageSize())), chunks(1)
    {
    }

    ~Arena()
    {
        delete head;

        if (recycle_enabled)
            for (size_t i = 0; i < free_lists.size(); ++i)
                delete free_lists[i].chunk;
    }

    /// Get piece of memory, without alignment.
    char * alloc(size_t size)
    {
        if (unlikely(head->pos + size > head->end))
            addMemoryChunk(size);

        char * res = head->pos;
        head->pos += size;

        /// proton: starts
        head->timestamp = current_timestamp;
        /// proton: ends

        ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
        return res;
    }

    /// Get piece of memory with alignment
    char * alignedAlloc(size_t size, size_t alignment)
    {
        do
        {
            void * head_pos = head->pos;
            size_t space = head->end - head->pos;

            auto * res = static_cast<char *>(std::align(alignment, size, head_pos, space));
            if (res)
            {
                head->pos = static_cast<char *>(head_pos);
                head->pos += size;

                /// proton: starts
                head->timestamp = current_timestamp;
                /// proton: ends

                ASAN_UNPOISON_MEMORY_REGION(res, size + pad_right);
                return res;
            }

            addMemoryChunk(size + alignment);
        } while (true);
    }

    template <typename T>
    T * alloc()
    {
        return reinterpret_cast<T *>(alignedAlloc(sizeof(T), alignof(T)));
    }

    /** Rollback just performed allocation.
      * Must pass size not more that was just allocated.
      * Return the resulting head pointer, so that the caller can assert that
      * the allocation it intended to roll back was indeed the last one.
      */
    void * rollback(size_t size)
    {
        head->pos -= size;
        ASAN_POISON_MEMORY_REGION(head->pos, size + pad_right);
        return head->pos;
    }

    /** Begin or expand a contiguous range of memory.
      * 'range_start' is the start of range. If nullptr, a new range is
      * allocated.
      * If there is no space in the current MemoryChunk to expand the range,
      * the entire range is copied to a new, bigger memory MemoryChunk, and the value
      * of 'range_start' is updated.
      * If the optional 'start_alignment' is specified, the start of range is
      * kept aligned to this value.
      *
      * NOTE This method is usable only for the last allocation made on this
      * Arena. For earlier allocations, see 'realloc' method.
      */
    char * allocContinue(size_t additional_bytes, char const *& range_start,
                         size_t start_alignment = 0)
    {
        /*
         * Allocating zero bytes doesn't make much sense. Also, a zero-sized
         * range might break the invariant that the range begins at least before
         * the current MemoryChunk end.
         */
        assert(additional_bytes > 0);

        if (!range_start)
        {
            // Start a new memory range.
            char * result = start_alignment
                ? alignedAlloc(additional_bytes, start_alignment)
                : alloc(additional_bytes);

            range_start = result;
            return result;
        }

        // Extend an existing memory range with 'additional_bytes'.

        // This method only works for extending the last allocation. For lack of
        // original size, check a weaker condition: that 'begin' is at least in
        // the current MemoryChunk.
        assert(range_start >= head->begin);
        assert(range_start < head->end);

        if (head->pos + additional_bytes <= head->end)
        {
            // The new size fits into the last MemoryChunk, so just alloc the
            // additional size. We can alloc without alignment here, because it
            // only applies to the start of the range, and we don't change it.
            return alloc(additional_bytes);
        }

        // New range doesn't fit into this MemoryChunk, will copy to a new one.
        //
        // Note: among other things, this method is used to provide a hack-ish
        // implementation of realloc over Arenas in ArenaAllocators. It wastes a
        // lot of memory -- quadratically so when we reach the linear allocation
        // threshold. This deficiency is intentionally left as is, and should be
        // solved not by complicating this method, but by rethinking the
        // approach to memory management for aggregate function states, so that
        // we can provide a proper realloc().
        const size_t existing_bytes = head->pos - range_start;
        const size_t new_bytes = existing_bytes + additional_bytes;
        const char * old_range = range_start;

        char * new_range = start_alignment
            ? alignedAlloc(new_bytes, start_alignment)
            : alloc(new_bytes);

        memcpy(new_range, old_range, existing_bytes);

        range_start = new_range;
        return new_range + existing_bytes;
    }

    /// NOTE Old memory region is wasted.
    char * realloc(const char * old_data, size_t old_size, size_t new_size)
    {
        char * res = alloc(new_size);
        if (old_data)
        {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    char * alignedRealloc(const char * old_data, size_t old_size, size_t new_size, size_t alignment)
    {
        char * res = alignedAlloc(new_size, alignment);
        if (old_data)
        {
            memcpy(res, old_data, old_size);
            ASAN_POISON_MEMORY_REGION(old_data, old_size);
        }
        return res;
    }

    /// Insert string without alignment.
    const char * insert(const char * data, size_t size)
    {
        char * res = alloc(size);
        memcpy(res, data, size);
        return res;
    }

    const char * alignedInsert(const char * data, size_t size, size_t alignment)
    {
        char * res = alignedAlloc(size, alignment);
        memcpy(res, data, size);
        return res;
    }

    /// Size of MemoryChunks in bytes.
    size_t size() const
    {
        return size_in_bytes;
    }

    /// Bad method, don't use it -- the MemoryChunks are not your business, the entire
    /// purpose of the arena code is to manage them for you, so if you find
    /// yourself having to use this method, probably you're doing something wrong.
    size_t remainingSpaceInCurrentMemoryChunk() const
    {
        return head->remaining();
    }

    /// proton: starts
    size_t numOfChunks() const
    {
        return chunks;
    }

    void enableRecycle(bool enable_recycle)
    {
        recycle_enabled = enable_recycle;
    }

    void setCurrentTimestamp(Int64 timestamp)
    {
        if (timestamp > current_timestamp)
            current_timestamp = timestamp;
    }

    /// If a Chunk's max timestamp < timestamp, it is good to recycle it
    struct Stats
    {
        size_t chunks = 0;
        size_t bytes = 0;
        size_t chunks_removed = 0;
        size_t bytes_removed = 0;
        size_t chunks_reused = 0;
        size_t bytes_reused = 0;
        size_t head_chunk_size = 0;
        size_t free_list_hits = 0;
        size_t free_list_misses = 0;
    };

    Stats free(Int64 timestamp)
    {
        assert(head);
        assert(recycle_enabled);

        Stats stats;

        /// `head` points to the largest timestamp
        /// Walk through the chunk list to find the first chunk which has timestamp less than argument `timestamp`
        auto * prev_p = head;
        auto * p = head;
        while (p)
        {
            if (p->timestamp >= timestamp)
            {
                prev_p = p;
                p = p->prev;
            }
            else
                break;
        }

        /// If p is head, only recycle head when its size reaches 2 pages
        if (p && ((p != head) || (p == head && p->size() > page_size * 2)))
        {
            /// Free all chunks starting from the first chunk which has timestamp less than argument `timestamp`
            auto * pp = p;
            while (pp)
            {
                size_in_bytes -= pp->size();
                --chunks;
                pp = recycle(pp, stats);
            }

            prev_p->prev = nullptr; /// NOLINT(clang-analyzer-cplusplus.NewDelete)

            if (p == head)
                head = new MemoryChunk(page_size, nullptr, std::numeric_limits<Int64>::min());
        }

        assert(head);

        stats.head_chunk_size = head->size(); /// NOLINT(clang-analyzer-core.CallAndMessage)
        stats.bytes = size_in_bytes;
        stats.chunks = chunks;
        stats.free_list_hits = free_list_hits;
        stats.free_list_misses = free_list_misses;

        return stats;
    }

private:
    MemoryChunk * recycle(MemoryChunk * chunk, Stats & stats)
    {
        assert(chunk);
        assert(recycle_enabled);

        MemoryChunk * prev = chunk->prev;
        chunk->prev = nullptr;

        auto index = bitScanReverse(chunk->size() / page_size);
        if (index >= free_lists.size() || free_lists[index].chunk)
        {
            /// 1) If the chunk is super big, let's free it
            /// 2) There is already having one of this size in free list
            /// We want to keep only one in each free list slot, so free this one
            ++stats.chunks_removed;
            stats.bytes_removed += chunk->size();
            delete chunk;
            return prev;
        }

        /// FIXME, memset it to zero ? asan
        /// FIXME, recycle if large chunk is not reused / accessed for a long time ?
        chunk->pos = chunk->begin;
        free_lists[index].chunk = chunk;
        free_lists[index].time_in_free_list = DB::MonotonicMilliseconds::now();
        ++chunks_in_free_lists;
        size_in_bytes_in_free_lists += chunk->size();

        ++stats.chunks_reused;
        stats.bytes_reused += chunk->size();

        return prev;
    }
    /// proton: ends
};

using ArenaPtr = std::shared_ptr<Arena>;
using Arenas = std::vector<ArenaPtr>;


}
