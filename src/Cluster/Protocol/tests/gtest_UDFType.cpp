#include <Cluster/Protocol/UDFType.h>
#include <Common/Exception.h>
#include <Cluster/Common/Error.h>

#include <gtest/gtest.h>

namespace cluster::protocol
{

TEST(UDFTypeTest, ToString)
{
    EXPECT_EQ(udf(UDFType::Executable), "executable");
    EXPECT_EQ(udf(UDFType::Remote), "remote");
    EXPECT_EQ(udf(UDFType::Javascript), "javascript");
    EXPECT_EQ(udf(UDFType::Python), "python");
    EXPECT_EQ(udf(UDFType::SQL), "sql");
}

TEST(UDFTypeTest, FromString)
{
    EXPECT_EQ(udf("executable"), UDFType::Executable);
    EXPECT_EQ(udf("remote"), UDFType::Remote);
    EXPECT_EQ(udf("javascript"), UDFType::Javascript);
    EXPECT_EQ(udf("python"), UDFType::Python);
    EXPECT_EQ(udf("sql"), UDFType::SQL);
}

}
