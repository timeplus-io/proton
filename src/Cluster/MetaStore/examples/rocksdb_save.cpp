#include <filesystem>
#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

/// @brief below shows rocksdb::writeBatch insert (old_key, new_data) will get new_data as fetching
int main()
{
    std::string db_path = "./testdb";

    if (!std::filesystem::exists(db_path))
        std::filesystem::create_directories(db_path);

    rocksdb::DB * db;
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);


    if (!status.ok())
    {
        std::cerr << "cannot open database: " << status.ToString() << std::endl;
        return 1;
    }

    std::string key = "old_key";
    std::string initial_data = "old_data";
    db->Put(rocksdb::WriteOptions(), key, initial_data);

    rocksdb::WriteBatch batch;
    std::string new_data = "new_data";
    batch.Put(key, new_data);
    db->Write(rocksdb::WriteOptions(), &batch);

    std::string value;
    status = db->Get(rocksdb::ReadOptions(), key, &value);
    if (status.ok())
        std::cout << "key: " << key << ", value: " << value << std::endl;
    else
        std::cerr << "fail to get data: " << status.ToString() << std::endl;

    delete db;
    std::filesystem::remove_all(db_path);
    return 0;
}
