#include <KafkaLog/KafkaWALProperties.h>

#include <gtest/gtest.h>

#include <unordered_set>

TEST(ParseProperties, empty)
{
    String exp;
    auto p = klog::parseProperties(exp);
    EXPECT_EQ(0, p.size());
}

TEST(ParseProperties, justSemicolons)
{
    String exp = ";;;;";
    auto p = klog::parseProperties(exp);
    EXPECT_EQ(0, p.size());
}

TEST(ParseProperties, singleProperty)
{
    std::unordered_set<String> cases{
        "key.1=value.1",
        ";key.1=value.1",
        "key.1=value.1;",
        ";key.1=value.1;",
        ";;key.1=value.1",
        "key.1=value.1;;",
        ";;key.1=value.1;;",
    };

    for (const auto & exp : cases)
    {
        auto p = klog::parseProperties(exp);
        EXPECT_EQ(1, p.size());

        auto kv = p.at(0);
        EXPECT_EQ("key.1", kv.first);
        EXPECT_EQ("value.1", kv.second);
    }
}

TEST(ParseProperties, multipleProperties)
{
    std::unordered_set<String> cases{
        "key.1=value.1;        key.2=value.2;key.3=value.3",
        "     key.1=value.1;   ;key.2=value.2     ;     key.3=value.3        ",
        "key.1=value.1;key.2=value.2;key.3=value.3;",
        ";;key.1=value.1;key.2=value.2;key.3=value.3;;",
    };

    for (const auto & exp : cases)
    {
        auto p = klog::parseProperties(exp);
        EXPECT_EQ(3, p.size());

        auto kv = p.at(0);
        EXPECT_EQ("key.1", kv.first);
        EXPECT_EQ("value.1", kv.second);
        kv = p.at(1);
        EXPECT_EQ("key.2", kv.first);
        EXPECT_EQ("value.2", kv.second);
        kv = p.at(2);
        EXPECT_EQ("key.3", kv.first);
        EXPECT_EQ("value.3", kv.second);
    }
}

TEST(ParseProperties, errorCases)
{
    std::unordered_set<String> cases{
        "key",
        "key=",
        "=value",
        "=",
    };

    for (const auto & exp : cases)
    {
        EXPECT_THROW(klog::parseProperties(exp), std::invalid_argument);
    }
}
