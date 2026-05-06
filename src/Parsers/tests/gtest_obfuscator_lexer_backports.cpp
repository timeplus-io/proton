/// proton: starts -- regression coverage for obfuscator (#97584, #101027) and Lexer (#103489) backports.
#include <Parsers/Lexer.h>
#include <Parsers/obfuscateQueries.h>
#include <Common/SipHash.h>
#include <IO/WriteBufferFromString.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
String runObfuscate(std::string_view src)
{
    WordMap obfuscate_map;
    WordSet used_nouns;
    SipHash hash_func;
    hash_func.update("test-seed");

    String result;
    {
        WriteBufferFromString out(result);
        obfuscateQueries(src, out, obfuscate_map, used_nouns, hash_func, [](std::string_view) { return false; });
    }
    return result;
}
}

/// [ClickHouse/ClickHouse#97584] inf/nan are special numeric literals; obfuscating them
/// yields unparseable SQL (e.g. SELECT -nutrient). Keep them as keywords.
TEST(ObfuscatorBackports, PreservesInfAndNanKeywords)
{
    const auto out = runObfuscate("SELECT inf, nan, -inf");
    EXPECT_NE(out.find("inf"), String::npos) << out;
    EXPECT_NE(out.find("nan"), String::npos) << out;
}

/// [ClickHouse/ClickHouse#97584] INTERVAL '2 years' is a single special form: the string
/// literal carries both a number and a unit. Pass it through verbatim.
TEST(ObfuscatorBackports, PreservesIntervalStringLiteral)
{
    const auto out = runObfuscate("SELECT INTERVAL '2 years'");
    EXPECT_NE(out.find("'2 years'"), String::npos) << out;
}

/// [ClickHouse/ClickHouse#97584] after_interval must survive insignificant tokens
/// (whitespace, block comments) so the string literal is still preserved.
TEST(ObfuscatorBackports, PreservesIntervalAcrossBlockComment)
{
    const auto out = runObfuscate("SELECT INTERVAL /* note */ '2 years'");
    EXPECT_NE(out.find("'2 years'"), String::npos) << out;
}

/// [ClickHouse/ClickHouse#97584] But a significant token between INTERVAL and the literal
/// resets the flag — the literal then takes the normal obfuscation path.
TEST(ObfuscatorBackports, IntervalFlagResetByPlus)
{
    const auto out = runObfuscate("SELECT INTERVAL + '2 years'");
    EXPECT_EQ(out.find("'2 years'"), String::npos) << out;
}

/// [ClickHouse/ClickHouse#101027] readIntText overflows uint64_t to zero on
/// extremely large numeric literals; the prior bitScanReverse(0) tripped an assert
/// in debug builds. The fix routes the zero case through writeIntText(obfuscated).
/// This test asserts only that the call returns without crashing.
TEST(ObfuscatorBackports, OverflowingNumericLiteralDoesNotAssert)
{
    EXPECT_NO_FATAL_FAILURE({
        const auto out = runObfuscate("SELECT 999999999999999999999999999999999999999");
        EXPECT_FALSE(out.empty());
    });
}

/// [ClickHouse/ClickHouse#103489] Lexer::nextToken pointer-arithmetic guard:
/// with a null begin and non-zero max_query_size, the old check
/// `res.end > begin + max_query_size` is UB on null pointers (UBSan-flagged).
/// The fix adds a `begin && ...` short-circuit. Verify the call returns cleanly
/// for an empty / null-buffer Lexer.
TEST(LexerBackports, NextTokenSurvivesNullBuffer)
{
    Lexer lexer(nullptr, nullptr, 100);
    auto token = lexer.nextToken();
    EXPECT_TRUE(token.isEnd() || token.isError()) << "type=" << static_cast<int>(token.type);
}

TEST(LexerBackports, NextTokenSurvivesEmptyBuffer)
{
    const char * empty = "";
    Lexer lexer(empty, empty, 100);
    auto token = lexer.nextToken();
    EXPECT_TRUE(token.isEnd() || token.isError()) << "type=" << static_cast<int>(token.type);
}
/// proton: ends
