#include <cassert>
#include <iostream>
#include <string>

#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>


std::string kDBPath = "/tmp/rocksdb_prefix_seek";

std::unique_ptr<rocksdb::DB> open(const std::string & db_path)
{
    rocksdb::Options db_options;
    db_options.atomic_flush = true;
    db_options.num_levels = 3;
    db_options.create_if_missing = true;
    db_options.create_missing_column_families = true;
    db_options.statistics = rocksdb::CreateDBStatistics();
    db_options.info_log_level = rocksdb::ERROR_LEVEL;
    db_options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(1));

    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

    db_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    rocksdb::DB * db = nullptr;

    if (auto status = rocksdb::DB::Open(db_options, db_path, &db); !status.ok())
    {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        std::abort();
    }

    return std::unique_ptr<rocksdb::DB>(db);
}

int main()
{
    std::unique_ptr<rocksdb::DB> db(open(kDBPath));

    /// Write
    {
        rocksdb::WriteBatch batch;
        batch.Put("a", "a");
        batch.Put("ab", "ab");
        batch.Put("a", "c");
        batch.Put("bc", "bc");
        batch.Put("aa", "aa");
        batch.Put("b", "b");

        rocksdb::WriteOptions write_options;
        write_options.disableWAL = true;

        if (auto status = db->Write(write_options, &batch); !status.ok())
        {
            std::cerr << "Failed to write batch: " << status.ToString() << std::endl;
            std::abort();
        }
    }

    /// Read
    {
        rocksdb::ReadOptions read_options;
        read_options.auto_prefix_mode = true;

        /// We can only iterate keys in primary index
        /// [lower_bound, upper_bound)
        std::string l = "a";
        std::string u = "b";
        rocksdb::Slice lower_bound = l;
        rocksdb::Slice upper_bound = u;
        read_options.iterate_upper_bound = &upper_bound;

        std::unique_ptr<rocksdb::Iterator> iterator(db->NewIterator(read_options));
        iterator->Seek(lower_bound);

        for (; iterator->Valid(); iterator->Next())
            std::cout << "key=" << iterator->key().ToStringView() << " value=" << iterator->key().ToStringView() << "\n";
    }

    return 0;
}
