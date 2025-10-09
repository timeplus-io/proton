#include <rocksdb/db.h>

#include <random>
#include <chrono>
#include <iostream>

int main(int argc, char ** argv)
{
    if (argc != 2)
    {
        std::cerr << "Usage: ./rocksdb_seek_perf <iterations>\n";
        return 1;
    }

    rocksdb::DB * db;
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.num_levels = 1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("position_offset", rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("atime_offset", rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("etime_offset", rocksdb::ColumnFamilyOptions()));
    std::vector<rocksdb::ColumnFamilyHandle*> handles;

    auto s = rocksdb::DB::Open(options, "./seek-perf", column_families, &handles, &db);
    if (!s.ok())
    {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return 1;
    }

    std::unique_ptr<rocksdb::DB> pdb{db};

    rocksdb::WriteOptions write_options;
    write_options.disableWAL = true;

    auto iterations = std::stoll(argv[1]);

    auto start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    for (int64_t offset = 0; offset < iterations; ++offset)
    {
        auto position = offset + 8192;

        rocksdb::Slice offset_s{reinterpret_cast<char *>(&offset), sizeof(offset)};
        rocksdb::Slice position_s{reinterpret_cast<char *>(&position), sizeof(position)};

        rocksdb::WriteBatch batch;

        /// Insert offset -> physical position mapping
        batch.Put(offset_s, position_s);

        /// Insert physical position -> offset mapping
        batch.Put(handles[0], position_s, offset_s);

        /// Insert event time -> offset mapping
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        now -= iterations - offset;

        rocksdb::Slice now_s = rocksdb::Slice{reinterpret_cast<char *>(&now), sizeof(now)};
        batch.Put(handles[1], now_s, offset_s);
        batch.Put(handles[2], now_s, offset_s);

        s = pdb->Write(write_options, &batch);
        if (!s.ok())
            std::cerr << "Failed to commit batch: " << s.ToString() << std::endl;
    }

    s = db->Flush(rocksdb::FlushOptions{});
    if (!s.ok())
    {
        std::cerr << "Failed to open database: " << s.ToString() << std::endl;
        return 1;
    }

    auto end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    std::cout << "Running " << iterations << " ingestion, took " << end - start << "ms, ips: " << iterations * 1000 / (end - start) << std::endl;

    {
        start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        /// Seek
        /// Seek offset -> physical position
        std::unique_ptr<rocksdb::Iterator> iter{pdb->NewIterator(rocksdb::ReadOptions{}, handles[0])};
        for (int64_t offset = 0; offset < iterations; ++offset)
        {
            /// Sequential seek forwards
            rocksdb::Slice key{reinterpret_cast<char *>(&offset), sizeof(offset)};
            /// <= key
            iter->SeekForPrev(key);
            assert(iter->Valid());
            assert(*reinterpret_cast<const int64_t*>(iter->key().data()) == offset);
            assert(*reinterpret_cast<const int64_t*>(iter->value().data()) == offset + 8192);
        }
        end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::cout << "Running " << iterations << " forward sequential seek, took " << end - start << "ms, ips: " << iterations * 1000 / (end - start) << std::endl;
    }

    {
        start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        /// Seek
        /// Seek offset -> physical position
        std::unique_ptr<rocksdb::Iterator> iter{pdb->NewIterator(rocksdb::ReadOptions{}, handles[0])};

        for (int64_t offset = 0; offset < iterations; ++offset)
        {
            /// Sequential seek backwards
            auto offset_b = iterations - offset - 1;
            rocksdb::Slice key{reinterpret_cast<char *>(&offset_b), sizeof(offset_b)};
            /// <= key
            iter->SeekForPrev(key);
            assert(iter->Valid());
            assert(*reinterpret_cast<const int64_t*>(iter->key().data()) == offset_b);
            assert(*reinterpret_cast<const int64_t*>(iter->value().data()) == offset_b + 8192);
        }
        end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::cout << "Running " << iterations << " backward sequential seek, took " << end - start << "ms, ips: " << iterations * 1000 / (end - start) << std::endl;
    }

    {
        /// Random, we first generate `iterations` random number
        std::vector<int64_t> offsets;
        offsets.reserve(iterations);
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<int64_t> distrib(0, iterations - 1);

        for (int i = 0; i < iterations; ++i)
            offsets.push_back(distrib(gen));

        start = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        /// Seek
        /// Seek offset -> physical position
        std::unique_ptr<rocksdb::Iterator> iter{pdb->NewIterator(rocksdb::ReadOptions{}, handles[0])};

        for (auto offset : offsets)
        {
            rocksdb::Slice key{reinterpret_cast<char *>(&offset), sizeof(offset)};
            /// <= key
            iter->SeekForPrev(key);
            assert(iter->Valid());
            assert(*reinterpret_cast<const int64_t*>(iter->key().data()) == offset);
            assert(*reinterpret_cast<const int64_t*>(iter->value().data()) == offset + 8192);
        }
        end = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::cout << "Running " << iterations << " random seek, took " << end - start << "ms, ips: " << iterations * 1000 / (end - start) << std::endl;
    }


    for (auto * handle : handles)
        pdb->DestroyColumnFamilyHandle(handle);

    return 0;
}
