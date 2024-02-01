#include <Common/tests/gtest_global_context.h>
#include <Storages/ExternalStream/ExternalStreamSettings.h>

#include <gtest/gtest.h>

TEST(ExternalStreamSettings, getFormatSettings)
{
    auto context = DB::Context::createCopy(getContext().context);
    context->setSetting("format_csv_delimiter", DB::Field("/"));

    DB::ExternalStreamSettings ex_stream_settings{};
    ex_stream_settings.format_csv_delimiter = ';';
    ex_stream_settings.bool_true_representation = "yes";

    auto settings = ex_stream_settings.getFormatSettings(context);
    EXPECT_EQ(settings.csv.delimiter, '/'); /// from settings_for_context
    EXPECT_EQ(settings.bool_true_representation, "yes"); /// from ex_stream_settings
    EXPECT_EQ(settings.bool_false_representation, "false"); /// default value
}
