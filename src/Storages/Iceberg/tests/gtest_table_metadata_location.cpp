#include <Storages/Iceberg/ICatalog.h>

#include <gtest/gtest.h>

using Apache::Iceberg::TableMetadata;

TEST(IcebergTableMetadata, LocationWithPathRoundTrips)
{
    TableMetadata metadata;
    metadata.withLocation().setLocation("s3://bucket/path/to/table");

    EXPECT_EQ(metadata.getLocation(/*path_only=*/false), "s3://bucket/path/to/table");
    EXPECT_EQ(metadata.getLocation(/*path_only=*/true), "path/to/table");
}

TEST(IcebergTableMetadata, BareBucketLocationHasNoTrailingSeparator)
{
    /// Every Amazon S3 Tables table location is a bare bucket root. A trailing separator
    /// here becomes `s3://bucket//metadata/...` in the committed manifest list.
    TableMetadata metadata;
    metadata.withLocation().setLocation("s3://0e1f2a3b-4c5d-6e7f--table-s3");

    EXPECT_EQ(metadata.getLocation(/*path_only=*/false), "s3://0e1f2a3b-4c5d-6e7f--table-s3");
    EXPECT_EQ(metadata.getLocation(/*path_only=*/true), "");
}

TEST(IcebergTableMetadata, LocationWithTrailingSlashHasNoTrailingSeparator)
{
    TableMetadata metadata;
    metadata.withLocation().setLocation("s3://bucket/");

    EXPECT_EQ(metadata.getLocation(/*path_only=*/false), "s3://bucket");
    EXPECT_EQ(metadata.getLocation(/*path_only=*/true), "");
}
