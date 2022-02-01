// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cassert>
#include <iostream>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>

std::string kDBPath = "/tmp/rocksdb_simple_example";

class PutIfAbsentOperator : public rocksdb::AssociativeMergeOperator
{
public:
    bool Merge(
        const rocksdb::Slice & key,
        const rocksdb::Slice * existing_value,
        const rocksdb::Slice & value,
        std::string * new_value,
        rocksdb::Logger * logger) const override
    {
        if (existing_value)
        {
            rocksdb::Log(logger, "key=%s already exists", key.ToString().c_str());
            return false;
        }
        else
        {
            *new_value = value.ToString();
            return true;
        }
    }

    const char * Name() const override { return "PutIfAbsentOperator"; }
};

std::unique_ptr<rocksdb::DB> open(const std::string & db_path)
{
    rocksdb::DB * db;
    rocksdb::Options options;
    // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.merge_operator.reset(new PutIfAbsentOperator);

    // open DB
    auto s = rocksdb::DB::Open(options, db_path, &db);
    assert(s.ok());

    return std::unique_ptr<rocksdb::DB>(db);
}

void readWrite(const std::unique_ptr<rocksdb::DB> & db)
{
    // Put key-value
    auto s = db->Put(rocksdb::WriteOptions(), "key1", "value");
    assert(s.ok());
    std::string value;
    // get value
    s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    assert(s.ok());
    assert(value == "value");

    // atomically apply a set of updates
    {
        rocksdb::WriteBatch batch;
        batch.Delete("key1");
        batch.Put("key2", value);
        s = db->Write(rocksdb::WriteOptions(), &batch);
    }

    s = db->Get(rocksdb::ReadOptions(), "key1", &value);
    assert(s.IsNotFound());

    db->Get(rocksdb::ReadOptions(), "key2", &value);
    assert(value == "value");

    {
        rocksdb::PinnableSlice pinnable_val;
        db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
    }

    {
        std::string string_val;
        // If it cannot pin the value, it copies the value to its internal buffer.
        // The intenral buffer could be set during construction.
        rocksdb::PinnableSlice pinnable_val(&string_val);
        db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
        assert(pinnable_val == "value");
        // If the value is not pinned, the internal buffer must have the value.
        assert(pinnable_val.IsPinned() || string_val == "value");
    }

    rocksdb::PinnableSlice pinnable_val;
    s = db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), "key1", &pinnable_val);
    assert(s.IsNotFound());
    // Reset PinnableSlice after each use and before each reuse
    pinnable_val.Reset();
    db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), "key2", &pinnable_val);
    assert(pinnable_val == "value");
    pinnable_val.Reset();
    // The Slice pointed by pinnable_val is not valid after this point
}

void putIfAbsent(const std::unique_ptr<rocksdb::DB> & db)
{
    auto status = db->Merge(rocksdb::WriteOptions{}, "k1", "v1");
    if (status.ok())
    {
        std::string value;
        status = db->Get(rocksdb::ReadOptions{}, "k1", &value);
        assert(status.ok());
        std::cout << "First merge successfully - value=" << value << std::endl;
    }
    else
    {
        std::cout << "First merge failed" << std::endl;
    }

    status = db->Merge(rocksdb::WriteOptions{}, "k1", "v2");
    if (status.ok())
    {
        std::string value;
        status = db->Get(rocksdb::ReadOptions{}, "k1", &value);
        assert(status.ok());
        std::cout << "Second merge successfully - value=" << value << std::endl;
    }
    else
    {
        std::string value;
        status = db->Get(rocksdb::ReadOptions{}, "k1", &value);
        assert(status.ok());
        std::cout << "First merge failed - value=" << value << std::endl;
    }
}

int main()
{
    std::unique_ptr<rocksdb::DB> db(open(kDBPath));

    readWrite(db);
    putIfAbsent(db);

    return 0;
}
