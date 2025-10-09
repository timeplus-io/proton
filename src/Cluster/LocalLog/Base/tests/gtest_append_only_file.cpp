#include <Cluster/LocalLog/Base/AppendOnlyFile.h>

#include <gtest/gtest.h>

TEST(AppendOnlyFile, PartialWrite)
{
    cluster::nlog::AppendOnlyFile file(
        "./big_file", /*file_already_exists=*/false, /*read_only_=*/false, /*initial_file_size=*/0, /*preallocate=*/false);

    std::vector<cluster::ByteVector> batch_data;

    size_t total_bytes = 1024 * 1024 * 1024ull;
    for (size_t i = 0; i < 64; ++i)
        batch_data.emplace_back(total_bytes / 64);

    auto res = file.append(batch_data, total_bytes);
    ASSERT_TRUE(!res.hasError());
    ASSERT_EQ(res.result, total_bytes);
}
