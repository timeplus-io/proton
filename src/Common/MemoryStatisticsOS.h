#pragma once
#if defined(OS_LINUX)
#include <cstdint>


namespace DB
{

/** Opens a file /proc/self/status. Keeps it open and reads memory statistics via 'pread'.
  * This is Linux specific.
  * See: man procfs
  *
  * Note: a class is used instead of a single function to avoid excessive file open/close on every use.
  * pread is used to avoid lseek.
  *
  * Actual performance is from 1 to 5 million iterations per second.
  */
class MemoryStatisticsOS
{
public:
    /// In number of bytes.
    struct Data
    {
        uint64_t virt;
        uint64_t resident;
        uint64_t shared;
        uint64_t code;
        uint64_t data_and_stack;

        /// Extra decomposition sourced from /proc/self/smaps_rollup when available
        /// (Linux >= 4.14). Useful when the process embeds a second allocator
        /// that jemalloc does not see — e.g. the mimalloc heap owned by the
        /// free-threaded CPython runtime. Fields stay at zero when smaps_rollup
        /// is unreadable on this kernel; check `smaps_rollup_available` to
        /// distinguish "genuinely zero" from "unavailable". Values are in bytes.
        bool smaps_rollup_available = false;
        uint64_t anonymous = 0;           /// heap + thread stacks + anon mmaps (incl. mimalloc pages)
        uint64_t anon_huge_pages = 0;     /// subset of `anonymous` backed by transparent huge pages
        uint64_t private_dirty = 0;       /// private pages that would be lost if the process died
        uint64_t swap = 0;                /// resident pages that have been swapped out
        uint64_t pss = 0;                 /// proportional set size (shared pages attributed fractionally)
    };

    MemoryStatisticsOS();
    ~MemoryStatisticsOS();

    /// Thread-safe.
    Data get() const;

private:
    int fd;
    int rollup_fd = -1;
};

}

#endif
