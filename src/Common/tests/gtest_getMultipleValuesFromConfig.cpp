#include <Common/getMultipleKeysFromConfig.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>


using namespace DB;

TEST(Common, getMultipleValuesFromConfig)
{
    std::istringstream      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        xml_isteam(R"END(<?xml version="1.0"?>
<proton>
    <first_level>
        <second_level>0</second_level>
        <second_level>1</second_level>
        <second_level>2</second_level>
        <second_level>3</second_level>
    </first_level>
</proton>)END");

    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(xml_isteam);
    std::vector<std::string> answer = getMultipleValuesFromConfig(*config, "first_level", "second_level");
    std::vector<std::string> right_answer = {"0", "1", "2", "3"};
    EXPECT_EQ(answer, right_answer);
}
