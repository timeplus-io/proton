#include <Storages/ExternalStream/ExternalStreamSettings.h>

#include <gtest/gtest.h>

TEST(ExternalStreamSettings, getFormatSettings)
{
    DB::Settings settings_for_context{};
    settings_for_context.format_csv_delimiter = '/';
    auto context = std::make_shared<DB::Context>();
    context->setSettings(settings_for_context);

    DB::ExternalStreamSettings ex_stream_settings{};
    ex_stream_settings.format_csv_delimiter = ';';
    ex_stream_settings.bool_true_representation = "yes";

    auto settings = ex_stream_settings.getFormatSettings(context);
    EXPECT_EQ(settings.csv.delimiter, '/'); /// from settings_for_context
    EXPECT_EQ(settings.bool_true_representation, "yes"); /// from ex_stream_settings
    EXPECT_EQ(settings.bool_false_representation, "false"); /// default value
}
