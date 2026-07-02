#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include <unistd.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <base/scope_guard.h>

namespace
{
std::string writeTempFile(const std::string & stem, const std::string & contents)
{
    auto name = fmt::format("{}_{}.txt", stem, ::getpid());
    auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream ofs(path);
    ofs << contents;
    ofs.close();
    return path.string();
}
}

/// After cancel() (e.g. triggered when next() throws), rewind() must clear the canceled flag so
/// the buffer can be reused for another read cycle. Long-lived readers (e.g. AsynchronousMetrics
/// sensor files) rely on this invariant; otherwise `chassert(!isCanceled())` in ReadBuffer::next()
/// fires on the next cycle.
TEST(ReadBufferFromFileRewind, ClearsCanceledFlag)
{
    auto path = writeTempFile("gtest_readbuffer_rewind", "42");
    SCOPE_EXIT({ std::filesystem::remove(path); });

    DB::ReadBufferFromFile buf(path);

    buf.cancel();
    ASSERT_TRUE(buf.isCanceled());

    buf.rewind();
    ASSERT_FALSE(buf.isCanceled());

    Int64 value = 0;
    DB::readText(value, buf);
    ASSERT_EQ(value, 42);
}
