#include <Cluster/Protocol/StreamDescriptor.h>

#include <gtest/gtest.h>

TEST(StreamDescription, Flags)
{
    cluster::protocol::StreamDescriptor desc;
    desc.withStartCommit();
    ASSERT_TRUE(desc.isStartCommit());

    desc.withCommit();
    ASSERT_TRUE(desc.isCommitted());
    ASSERT_FALSE(desc.isStartCommit());
}
