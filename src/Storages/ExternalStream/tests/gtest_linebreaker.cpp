#include <Storages/ExternalStream/BreakLines.h>

#include <re2/re2.h>
#include <gtest/gtest.h>

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
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+))";
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
    std::string line1{"2022.04.14 18:04:43.028648 [ 4273 ] {} <Information> TablesLoader: Parsed metadata of 0 tables in 2 databases in 0.000351319 sec\n"};
    std::string line2{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables.\n"};
    std::string line3{"2022.04.14 18:04:43.028801 [ 4273 ] {} <Information> DatabaseAtomic (default): Starting up tables."};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+))";
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

TEST(BreakLines, BreakLinesHasPrefix)
{
    std::string line1{"Processing configuration file 'config.xml'.There is no file 'config.xml', will use embedded config.Env variable is "
                      "not set: TELEMETRY_ENABLED\n"};
    std::string line2{"2024.06.03 14:19:48.818674 [ 374515 ] {} <Information> SentryWriter: Sending crash reports is disabled\n"};
    std::string line3{"2024.06.03 14:19:49.402200 [ 374515 ] {} <Information> Application: starting up\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    EXPECT_FALSE(re2::RE2::FindAndConsume(&input, line_breaker));

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

TEST(BreakLines, BreakLinesHasException)
{
    std::string line1{
        "2024.06.03 14:20:30.158783 [ 374526 ] {82a4387a-8af0-491f-aa1c-57ab1e2f7359} <Error> TCPHandler: Code: 81. DB::Exception: "
        "Database proton doesn't exist. (UNKNOWN_DATABASE), Stack trace (when copying this message, always include the lines below):\n"
        "\n"
        "0. /home/chh/proton/contrib/llvm-project/libcxx/include/exception:134: std::exception::capture() @ 0x0000000019ff9562 in "
        "/home/chh/proton/build/programs/proton\n"
        "1. /home/chh/proton/contrib/llvm-project/libcxx/include/exception:112: std::exception::exception[abi:v15000]() @ "
        "0x0000000019ff952d in /home/chh/proton/build/programs/proton\n"
        "2. /home/chh/proton/base/poco/Foundation/src/Exception.cpp:27: Poco::Exception::Exception(std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>> const&, int) @ 0x000000003288e040 in "
        "/home/chh/proton/build/programs/proton\n"
        "3. /home/chh/proton/src/Common/Exception.cpp:67: DB::Exception::Exception(std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>> const&, int, bool) @ 0x0000000022dc642e in "
        "/home/chh/proton/build/programs/proton\n"
        "4. /home/chh/proton/src/Interpreters/DatabaseCatalog.cpp:302: "
        "DB::DatabaseCatalog::assertDatabaseExistsUnlocked(std::__1::basic_string<char, std::__1::char_traits<char>, "
        "std::__1::allocator<char>> const&) const @ 0x000000002af55d92 in /home/chh/proton/build/programs/proton\n"
        "5. /home/chh/proton/src/Interpreters/DatabaseCatalog.cpp:409: DB::DatabaseCatalog::getDatabase(std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>> const&) const @ 0x000000002af580d2 in "
        "/home/chh/proton/build/programs/proton\n"
        "6. /home/chh/proton/src/Interpreters/Context.cpp:3120: DB::Context::resolveStorageID(DB::StorageID, "
        "DB::Context::StorageNamespace) const @ 0x000000002ae7b69c in /home/chh/proton/build/programs/proton\n"
        "7. /home/chh/proton/src/Interpreters/JoinedTables.cpp:214: DB::JoinedTables::getLeftTableStorage() @ 0x000000002b47bc72 in "
        "/home/chh/proton/build/programs/proton\n"
        "8. /home/chh/proton/src/Interpreters/InterpreterSelectQuery.cpp:500: "
        "DB::InterpreterSelectQuery::InterpreterSelectQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context> "
        "const&, std::__1::optional<DB::Pipe>, std::__1::shared_ptr<DB::IStorage> const&, DB::SelectQueryOptions const&, "
        "std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, "
        "std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> const&, "
        "std::__1::shared_ptr<DB::StorageInMemoryMetadata const> const&, std::__1::shared_ptr<DB::PreparedSets>)::$_0::operator()() const "
        "@ 0x000000002b3887a3 in /home/chh/proton/build/programs/proton\n"
        "9. /home/chh/proton/src/Interpreters/InterpreterSelectQuery.cpp:580: "
        "DB::InterpreterSelectQuery::InterpreterSelectQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context> "
        "const&, std::__1::optional<DB::Pipe>, std::__1::shared_ptr<DB::IStorage> const&, DB::SelectQueryOptions const&, "
        "std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, "
        "std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> const&, "
        "std::__1::shared_ptr<DB::StorageInMemoryMetadata const> const&, std::__1::shared_ptr<DB::PreparedSets>) @ 0x000000002b386500 in "
        "/home/chh/proton/build/programs/proton\n"
        "10. /home/chh/proton/src/Interpreters/InterpreterSelectQuery.cpp:276: "
        "DB::InterpreterSelectQuery::InterpreterSelectQuery(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context> "
        "const&, DB::SelectQueryOptions const&, std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, "
        "std::__1::allocator<char>>, std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, "
        "std::__1::allocator<char>>>> const&) @ 0x000000002b385626 in /home/chh/proton/build/programs/proton\n"
        "11. /home/chh/proton/contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:714: "
        "std::__1::__unique_if<DB::InterpreterSelectQuery>::__unique_single std::__1::make_unique[abi:v15000]<DB::InterpreterSelectQuery, "
        "std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context>&, DB::SelectQueryOptions&, "
        "std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, "
        "std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> "
        "const&>(std::__1::shared_ptr<DB::IAST> const&, std::__1::shared_ptr<DB::Context>&, DB::SelectQueryOptions&, "
        "std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, "
        "std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> const&) @ "
        "0x000000002b37b4a1 in /home/chh/proton/build/programs/proton\n"
        "12. /home/chh/proton/src/Interpreters/InterpreterSelectWithUnionQuery.cpp:237: "
        "DB::InterpreterSelectWithUnionQuery::buildCurrentChildInterpreter(std::__1::shared_ptr<DB::IAST> const&, "
        "std::__1::vector<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>, "
        "std::__1::allocator<std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>>>> const&) @ "
        "0x000000002b37756f in /home/chh/proton/build/programs/proton\n"
        "13. /home/chh/proton/src/Interpreters/InterpreterSelectWithUnionQuery.cpp:157: "
        "DB::InterpreterSelectWithUnionQuery::InterpreterSelectWithUnionQuery(std::__1::shared_ptr<DB::IAST> const&, "
        "std::__1::shared_ptr<DB::Context> const&, DB::SelectQueryOptions const&, std::__1::vector<std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>>, std::__1::allocator<std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>>>> const&) @ 0x000000002b376d7f in /home/chh/proton/build/programs/proton\n"
        "14. /home/chh/proton/contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:714: "
        "std::__1::__unique_if<DB::InterpreterSelectWithUnionQuery>::__unique_single "
        "std::__1::make_unique[abi:v15000]<DB::InterpreterSelectWithUnionQuery, std::__1::shared_ptr<DB::IAST>&, "
        "std::__1::shared_ptr<DB::Context>&, DB::SelectQueryOptions const&>(std::__1::shared_ptr<DB::IAST>&, "
        "std::__1::shared_ptr<DB::Context>&, DB::SelectQueryOptions const&) @ 0x000000002b8a388b in "
        "/home/chh/proton/build/programs/proton\n"
        "15. /home/chh/proton/src/Interpreters/InterpreterFactory.cpp:136: DB::InterpreterFactory::get(std::__1::shared_ptr<DB::IAST>&, "
        "std::__1::shared_ptr<DB::Context>, DB::SelectQueryOptions const&) @ 0x000000002b8a1a67 in /home/chh/proton/build/programs/proton\n"
        "16. /home/chh/proton/src/Interpreters/executeQuery.cpp:738: DB::executeQueryImpl(char const*, char const*, "
        "std::__1::shared_ptr<DB::Context>, bool, DB::QueryProcessingStage::Enum, DB::ReadBuffer*) @ 0x000000002b88e068 in "
        "/home/chh/proton/build/programs/proton\n"
        "17. /home/chh/proton/src/Interpreters/executeQuery.cpp:1155: DB::executeQuery(std::__1::basic_string<char, "
        "std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::shared_ptr<DB::Context>, bool, "
        "DB::QueryProcessingStage::Enum) @ 0x000000002b88b0a4 in /home/chh/proton/build/programs/proton\n"
        "18. /home/chh/proton/src/Server/TCPHandler.cpp:354: DB::TCPHandler::runImpl() @ 0x000000002c5b279f in "
        "/home/chh/proton/build/programs/proton\n"
        "19. /home/chh/proton/src/Server/TCPHandler.cpp:1871: DB::TCPHandler::run() @ 0x000000002c5c2545 in "
        "/home/chh/proton/build/programs/proton\n"
        "20. /home/chh/proton/base/poco/Net/src/TCPServerConnection.cpp:43: Poco::Net::TCPServerConnection::start() @ 0x00000000326d1d79 "
        "in /home/chh/proton/build/programs/proton\n"
        "21. /home/chh/proton/base/poco/Net/src/TCPServerDispatcher.cpp:115: Poco::Net::TCPServerDispatcher::run() @ 0x00000000326d25bc in "
        "/home/chh/proton/build/programs/proton\n"
        "22. /home/chh/proton/base/poco/Foundation/src/ThreadPool.cpp:199: Poco::PooledThread::run() @ 0x000000003291a8f4 in "
        "/home/chh/proton/build/programs/proton\n"
        "23. /home/chh/proton/base/poco/Foundation/src/Thread.cpp:56: Poco::(anonymous namespace)::RunnableHolder::run() @ "
        "0x000000003291769a in /home/chh/proton/build/programs/proton\n"
        "24. /home/chh/proton/base/poco/Foundation/src/Thread_POSIX.cpp:345: Poco::ThreadImpl::runnableEntry(void*) @ 0x000000003291639e "
        "in /home/chh/proton/build/programs/proton\n"
        "25. ? @ 0x00007b095da94ac3 in ?\n"
        "26. ? @ 0x00007b095db26850 in ?"};
    std::string line2{
        "2024.06.03 14:20:34.174067 [ 374526 ] {28589d1e-8340-4c94-a74c-09b94473a2d3} <Information> External-FileLog: Using 1 regexes: "
        "proton.log to search log_dir=/home/chh/proton/build/programs with start_timestamp=0, found 0 candidates\n"};
    std::string line3{"2024.06.03 14:20:34.175746 [ 374835 ] {28589d1e-8340-4c94-a74c-09b94473a2d3} <Information> PipelineExecutor: Using "
                      "20 threads to execute pipeline for query_id=28589d1e-8340-4c94-a74c-09b94473a2d3\n"};

    std::string pattern = R"((\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}\.\d+) \[ \d+ \] \{)";
    re2::RE2 line_breaker{pattern};

    re2::StringPiece input{line1};
    EXPECT_TRUE(re2::RE2::FindAndConsume(&input, line_breaker));

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
