#include <Cluster/Rocks/NamespaceKey.h>
#include <Common/scope_guard_safe.h>
#include <Common/Exception.h>

#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <filesystem>

TEST(NamespaceKey, NamespaceKey)
{
    EXPECT_EQ(cluster::namespaceKey(/*ns=*/std::string("ns"), /*ns_sep=*/'/'), "/ns");
    EXPECT_EQ(cluster::namespaceKey(/*ns=*/std::string("ns"), /*key=*/"streams", /*ns_sep=*/'/'), "/ns/streams");
}

TEST(NamespaceTransform, PrefixSeek)
{
    rocksdb::DB * db = nullptr;
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.num_levels = 1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    options.prefix_extractor = std::make_shared<cluster::NamespaceTransform>();

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));

    std::vector<rocksdb::ColumnFamilyHandle *> handles;

    std::error_code ec;
    std::filesystem::remove_all(std::filesystem::path("/tmp/rocks_namespace_seek"), ec);

    auto s = rocksdb::DB::Open(options, "/tmp/rocks_namespace_seek", column_families, &handles, &db);
    ASSERT_TRUE(s.ok());

    std::unique_ptr<rocksdb::DB> pdb{db};

    SCOPE_EXIT_SAFE({
        pdb->DestroyColumnFamilyHandle(handles[0]);
        pdb->Close();
    });

    rocksdb::WriteBatch batch;
    /// Write /m/lsn
    {
        auto key = cluster::namespaceKey(std::string("m"), "lsn");
        batch.Put(handles[0], key, "1");
    }

    /// Write /m/nds/1
    for (uint64_t n : {static_cast<uint64_t>(1), static_cast<uint64_t>(2), std::numeric_limits<uint64_t>::max() - 1})
    {
        auto key = cluster::namespaceKey(std::string("m"), "nds", std::string_view(reinterpret_cast<const char *>(&n), sizeof(n)));
        batch.Put(handles[0], key, fmt::format("{}", n));
    }

    /// Write /m/ods/0
    {
        auto key = cluster::namespaceKey(std::string("m"), "ods", "0");
        batch.Put(handles[0], key, "0");
    }

    /// Write
    {
        rocksdb::WriteOptions write_options;
        write_options.sync = true;

        auto status = pdb->Write(write_options, &batch);
        ASSERT_TRUE(status.ok());
    }

    {
        rocksdb::ReadOptions read_options;
        read_options.total_order_seek = true;
        read_options.prefix_same_as_start = true;

        /// numeric max is also alphabetic max for NodeID
        auto n = std::numeric_limits<uint64_t>::max();
        auto max_key = cluster::namespaceKey(std::string("m"), "nds", std::string_view(reinterpret_cast<char *>(&n), sizeof(n)));

        rocksdb::Slice max_key_slice = max_key;
        read_options.iterate_upper_bound = &max_key_slice;

        std::unique_ptr<rocksdb::Iterator> iter{pdb->NewIterator(read_options, handles[0])};

        auto prefix = cluster::namespaceKey(std::string("m"), "nds");
        iter->Seek(prefix);

        std::unordered_map<uint64_t, std::string> kvs;
        while (iter->Valid())
        {
            ASSERT_TRUE(iter->key().starts_with(prefix));
            kvs.emplace(*reinterpret_cast<const uint64_t *>(iter->key().data() + prefix.size() + 1), iter->value().ToString());
            iter->Next();
        }

        ASSERT_EQ(kvs.size(), 3);
	for (uint64_t k : {static_cast<uint64_t>(1), static_cast<uint64_t>(2), std::numeric_limits<uint64_t>::max() - 1})
        {
            ASSERT_TRUE(kvs.contains(k));
            ASSERT_EQ(kvs.at(k), fmt::format("{}", k));
        }
    }
}
