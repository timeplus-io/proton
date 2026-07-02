#include <Interpreters/InputSettingsUtils.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

namespace DB
{

TEST(InputSettingsUtils, ExtractTargetStreamsFromInputSettings)
{
    ParserCreateQuery parser;
    const std::string query
        = "CREATE INPUT test_db.test_input (`_raw` string) "
          "SETTINGS type = 'otel', logs_target_stream = 'test_db.logs', metrics_gauge_target_stream = 'test_db.metrics_gauge', "
          "target_stream = 'test_db.single', listen_host = '0.0.0.0'";

    auto ast = parseQuery(parser, query, /*max_query_size=*/0, /*max_parser_depth=*/0);
    const auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    ASSERT_NE(create, nullptr);
    ASSERT_TRUE(create->storage);
    ASSERT_TRUE(create->storage->settings);

    const auto targets = extractTargetStreamValues(create->storage->settings);
    EXPECT_EQ(
        targets,
        (std::vector<String>{
            "test_db.logs",
            "test_db.metrics_gauge",
            "test_db.single",
        }));
}

TEST(InputSettingsUtils, ExtractTargetStreamsSettingKeyIsCaseSensitive)
{
    ParserCreateQuery parser;
    const std::string query = "CREATE INPUT test_db.test_input (`_raw` string) "
                              "SETTINGS LoGs_TaRgEt_StReAm = 'test_db.logs', LISTEN_HOST = '0.0.0.0'";

    auto ast = parseQuery(parser, query, /*max_query_size=*/0, /*max_parser_depth=*/0);
    const auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    ASSERT_NE(create, nullptr);
    ASSERT_TRUE(create->storage);
    ASSERT_TRUE(create->storage->settings);

    const auto targets = extractTargetStreamValues(create->storage->settings);
    EXPECT_TRUE(targets.empty());
}

TEST(InputSettingsUtils, ExtractTargetStreamsFromOtelInputWithoutColumns)
{
    ParserCreateQuery parser;
    const std::string query = "CREATE INPUT otel_input "
                              "SETTINGS type = 'otel', protocol = 'grpc', tcp_port = 4317, logs_target_stream = 'otel_logs', "
                              "metrics_gauge_target_stream = 'otel_metrics_gauge', metrics_sum_target_stream = 'otel_metrics_sum', "
                              "metrics_histogram_target_stream = 'otel_metrics_histogram', metrics_exponential_histogram_target_stream = "
                              "'otel_metrics_exponential_histogram', "
                              "metrics_summary_target_stream = 'otel_metrics_summary', traces_target_stream = 'otel_traces'";

    auto ast = parseQuery(parser, query, /*max_query_size=*/0, /*max_parser_depth=*/0);
    const auto * create = ast ? ast->as<ASTCreateQuery>() : nullptr;
    ASSERT_NE(create, nullptr);
    ASSERT_TRUE(create->storage);
    ASSERT_TRUE(create->storage->settings);

    const auto targets = extractTargetStreamValues(create->storage->settings);
    EXPECT_EQ(
        targets,
        (std::vector<String>{
            "otel_logs",
            "otel_metrics_exponential_histogram",
            "otel_metrics_gauge",
            "otel_metrics_histogram",
            "otel_metrics_sum",
            "otel_metrics_summary",
            "otel_traces",
        }));
}

}
