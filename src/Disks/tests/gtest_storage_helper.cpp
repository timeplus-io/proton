#include <gtest/gtest.h>

#include <Cluster/Protocol/DiskDescriptor.h>
#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>
#include <Disks/StorageHelpers.h>
#include <Disks/DiskSelector.h>
#include <Parsers/ASTCreateDiskQuery.h>
#include <Parsers/ParserCreateDiskQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/tests/gtest_global_context.h>

#include <Poco/AutoPtr.h>

using namespace DB;

TEST(StorageHelperTest, ValidDiskConfiguration)
{
    auto context = getContext().context;

    String create_disk_str = R"###(
create disk mtest disk(
    type='s3',
    endpoint='http://localhost:11111/test/s3/',
    access_key_id='proton',
    secret_access_key='protonproton'
))###";

    ParserCreateDiskQuery parser;
    ASTPtr ast = parseQuery(parser, create_disk_str.data(), create_disk_str.data() + create_disk_str.size(), "", 0, 0);
    ASTCreateDiskQuery * create = ast->as<ASTCreateDiskQuery>();
    auto desc = getDiskDescriptorFromAST(*create, context);
    EXPECT_EQ("mtest", desc->name);
    EXPECT_EQ(cluster::protocol::DiskDescriptor::DiskType::S3, desc->type);
    EXPECT_EQ(
        "disk(type = 's3', endpoint = 'http://localhost:11111/test/s3/', access_key_id = 'proton', secret_access_key = 'protonproton')",
        desc->config);
}

TEST(StorageHelperTest, InvalidDiskConfiguration)
{
    auto context = getContext().context;

    String create_disk_str = R"###(
create disk mtest disk(
    type='s31',
    endpoint='http://localhost:11111/test/s3/',
    access_key_id='proton',
    secret_access_key='protonproton'
))###";

    ParserCreateDiskQuery parser;
    ASTPtr ast = parseQuery(parser, create_disk_str.data(), create_disk_str.data() + create_disk_str.size(), "", 0, 0);
    ASTCreateDiskQuery * create = ast->as<ASTCreateDiskQuery>();
    EXPECT_THROW(getDiskDescriptorFromAST(*create, context), DB::Exception);
}

TEST(DropDiskTest, DiskInUse)
{

}

TEST(DropDiskTest, DiskNotExists)
{
}
