#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/Protocol/InternalProtocol.h>

#include <gtest/gtest.h>

TEST(MetadataUpdater, MagicEnum)
{
    GTEST_ASSERT_EQ(1, magic_enum::enum_contains(cluster::protocol::OpCode::CreateStream));
    GTEST_ASSERT_EQ(1, magic_enum::enum_contains(cluster::protocol::OpCode::CreateUserDefinedFunction));

    GTEST_ASSERT_EQ("CreateStream", magic_enum::enum_name(cluster::protocol::OpCode::CreateStream));
    GTEST_ASSERT_EQ("CreateUserDefinedFunction", magic_enum::enum_name(cluster::protocol::OpCode::CreateUserDefinedFunction));
}
