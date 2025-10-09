#if defined(OS_LINUX)
#include <IO/ReadBufferFromString.h>
#include <Interpreters/AsynchronousMetrics.h>

#include <gtest/gtest.h>

TEST(AsynchronousMetrics, ReadSelfProcStatValues)
{
    DB::String proc_self_stat
        = "285148 (proton) S 30568 285148 30568 34823 285148 1077936128 480305 0 120 0 2203 180 0 0 20 0 307 0 1273043 22003733737472 "
          "1038505 18446744073709551615 94348156383232 94349233231136 140728777506672 0 0 0 4096 0 1074287871 0 0 0 17 18 0 0 0 0 0 "
          "94349247057920 94349287909952 94349311668224 140728777508196 140728777508264 140728777508264 140728777510869 0";

    auto buf = DB::ReadBufferFromString(proc_self_stat);

    DB::AsynchronousMetrics::SelfProcStats values{};
    values.read(buf);

    EXPECT_EQ(values.utime, 2203);
    EXPECT_EQ(values.stime, 180);
    EXPECT_EQ(values.cutime, 0);
    EXPECT_EQ(values.cstime, 0);
}
#endif
