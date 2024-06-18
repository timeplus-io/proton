#include <Storages/ExternalStream/BreakLines.h>

#include <gtest/gtest.h>
#include <re2/re2.h>

TEST(BreakLines, BreakLinesAtBeginning)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        auto length = line1.size();
        auto lines{DB::breakLines(line1.data(), length, pattern, 4096)};
        EXPECT_TRUE(lines.empty());
        EXPECT_EQ(length, line1.size());
    }

    {
        std::string data(line1 + line2);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 1);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(remaining, line2.size());
    }
}

TEST(BreakLines, BreakLinesAtEnd)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2})";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        auto remaining = line1.size();
        auto lines{DB::breakLines(line1.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 0);
        // EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(remaining, line1.size());
    }

    {
        std::string data(line1 + line2);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 1);
        EXPECT_EQ(line1, lines[0]);
        // EXPECT_EQ(line2, lines[1]);
        EXPECT_EQ(remaining, line2.size());
    }
}

TEST(BreakLines, BreakLinesLastLineIsNotClosedAtEnd)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};
    std::string line3{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2})";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2 + line3);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 2);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(line2, lines[1]);
        EXPECT_EQ(remaining, data.size() - line1.size() - line2.size());
        EXPECT_STREQ(data.c_str() + line1.size() + line2.size(), line3.c_str());
    }
}

TEST(BreakLines, BreakLinesLastLineIsNotClosedAtBeginning)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};
    std::string line3{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2 + line3);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 2);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(line2, lines[1]);
        EXPECT_EQ(remaining, data.size() - line1.size() - line2.size());
        EXPECT_STREQ(data.c_str() + line1.size() + line2.size(), line3.c_str());
    }
}

TEST(BreakLines, BreakLinesMaxLineSize)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};
    std::string line3{"022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2 + line3);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 10)};
        ASSERT_EQ(lines.size(), 2);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(line2 + line3, lines[1]);
        EXPECT_EQ(remaining, 0);
    }
}

TEST(BreakLines, BreakLinesMaxLineSizeForceFlush)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};
    std::string line3{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2 + line3);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 10)};
        ASSERT_EQ(lines.size(), 3);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(line2, lines[1]);
        EXPECT_EQ(line3, lines[2]);
        EXPECT_EQ(remaining, 0);
    }
}

TEST(BreakLines, BreakLinesMultipleLineRecord)
{
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\nException: abc\nstack trace:\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        ASSERT_EQ(lines.size(), 1);
        EXPECT_EQ(line1, lines[0]);
        EXPECT_EQ(remaining, data.size() - line1.size());
        EXPECT_STREQ(line2.c_str(), data.c_str() + line1.size());
    }
}

TEST(BreakLines, BreakLinesNoMatch)
{
    std::string line1{"022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\nException: abc\nstack trace:\n"};
    std::string line2{"022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_FALSE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        EXPECT_TRUE(lines.empty());
        EXPECT_EQ(remaining, data.size());
    }
}

TEST(BreakLines, BreakLinesHasConfigInfo)
{
    std::string line1{"Processing configuration file 'config.xml'.There is no file 'config.xml', will use embedded config.Env variable is "
                      "not set: TELEMETRY_ENABLED\n"};
    std::string line2{"2024.06.03 14:19:48.818674 [ 374515 ] {} <Information> SentryWriter: Sending crash reports is disabled"};
    std::string line3{"2024.06.03 14:19:49.402200 [ 374515 ] {} <Information> Application: starting up"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    ASSERT_FALSE(re2::RE2::FindAndConsume(&input, line_breaker));

    {
        std::string data(line1 + line2 + line3);
        auto remaining = data.size();
        auto lines{DB::breakLines(data.data(), remaining, pattern, 4096)};
        EXPECT_EQ(lines.size(), 2);
        EXPECT_EQ(lines[0], line1);
        EXPECT_EQ(lines[1], line2);
        EXPECT_EQ(remaining, line3.size());
    }
}
