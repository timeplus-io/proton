#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/table.h>

#include <iostream>
/// #include <chrono>
/// #include <thread>

const char NAMESPACE_SEPARATOR = '/';

class NamespacePrefixTransform : public rocksdb::SliceTransform
{
private:
    char separator;
    std::string name_;

public:
    explicit NamespacePrefixTransform(char separator_ = NAMESPACE_SEPARATOR)
        : separator(separator_), name_("Proton.rocksdb.NamespacePrefix")
    {
    }

    const char * Name() const override { return name_.c_str(); }

    /// prefix => top-namespace
    rocksdb::Slice Transform(const rocksdb::Slice & src) const override
    {
        auto begin = src.data();
        auto end = begin + src.size();
        auto pos = std::find(begin, end, separator);
        if (pos == end)
            return {};
        return rocksdb::Slice(begin, pos - begin);
    }

    bool InDomain(const rocksdb::Slice & src) const override
    {
        auto begin = src.data();
        auto end = begin + src.size();
        auto pos = std::find(begin, end, separator);
        if (pos == end)
            return false;
        return true;
    }

    bool SameResultWhenAppended(const rocksdb::Slice &) const override { return false; }
};

int main()
{
    rocksdb::Options options;
    rocksdb::DB * db;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.compression = rocksdb::CompressionType::kZSTD;

    /// Enable prefix seek
    options.prefix_extractor.reset(new NamespacePrefixTransform());

    /// Enable prefix bloom for mem tables, size: `write_buffer_size(default: 64MB) * memtable_prefix_bloom_size_ratio`
    options.memtable_whole_key_filtering = true;
    options.memtable_prefix_bloom_size_ratio = 0.1;

    /// Enable prefix bloom for SST file
    rocksdb::BlockBasedTableOptions table_options;
    table_options.whole_key_filtering = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10 /* bits_per_key */, true));
    options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    std::string rocksdb_dir = "./coordination";

    rocksdb::DBOptions db_options(options);
    rocksdb::ColumnFamilyOptions cf_options(options);
    std::vector<std::string> cf_names;

    auto status = rocksdb::DB::ListColumnFamilies(db_options, rocksdb_dir, &cf_names);
    if (status != rocksdb::Status::OK())
    {
        std::cerr << "Failed to list column family : " << status.ToString() << "\n";
        return -1;
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descriptors;
    for (auto & name : cf_names)
    {
        if (name == rocksdb::kDefaultColumnFamilyName)
            continue;

        cf_descriptors.emplace_back(name, cf_options);
    }

    cf_descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, cf_options); /// set default system column family

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handlers;
    cf_handlers.reserve(cf_descriptors.size() + 1);

    /// Open Rocksdb with column families
    status = rocksdb::DB::Open(options, rocksdb_dir, cf_descriptors, &cf_handlers, &db);
    if (status != rocksdb::Status::OK())
        return -1;

    std::unique_ptr<rocksdb::DB> rocksdb_ptr(db);

    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> column_families;
    assert(cf_descriptors.size() == cf_handlers.size());

    for (size_t i = 0; i < cf_descriptors.size(); ++i)
    {
        column_families.emplace(cf_descriptors[i].name, cf_handlers[i]);

        std::cerr << "Discovered " << cf_descriptors[i].name << " Column Family\n";
    }

    std::cerr << "Total " << cf_handlers.size() << " descriptors have been discovered\n";

    std::string prefix_begin = "query/";
    std::string prefix_end = "query/z";

    auto scan = [&](std::string_view msg) {
        std::cout << msg << "\n";
        size_t i = 0;
        size_t bytes = 0;

        std::unique_ptr<rocksdb::Iterator> iter{db->NewIterator(rocksdb::ReadOptions{}, column_families["neutron"])};
        iter->Seek(prefix_begin);

        while (iter->Valid())
        {
            if (iter->key().starts_with(prefix_begin))
            {
                ++i;
                bytes += iter->key().size() + iter->value().size();
                if (i % 100000 == 0)
                    std::cout << i << " " << bytes << "\n";
            }

            iter->Next();
        }

        std::cout << "Total keys " << i << " and total GB " << bytes / 1024.0 / 1024.0 / 1024.0 << "\n";
    };

    scan("Scanning...");

    rocksdb::Slice range_begin = prefix_begin;
    rocksdb::Slice range_end = prefix_end;

    std::cout << "Deleting key range ['" << prefix_begin << "', '" << prefix_end << "'], press any key to proceed: ";
    std::getchar();

    status = rocksdb::DeleteFilesInRange(db, column_families["neutron"], &range_begin, &range_end, true);
    if (status == status.OK())
        std::cerr << "Deleted range successfully\n";
    else
        std::cerr << "Failed to delete range " << status.ToString() << "\n";

    scan("Rescanning...");

#if 0
    {
        std::unique_ptr<rocksdb::Iterator> iter{db->NewIterator(rocksdb::ReadOptions{}, column_families["neutron"])};
        iter->SeekForPrev(range_end);

        i = 0;
        while (iter->Valid())
        {
            if (iter->key().starts_with(prefix_begin))
            {
                ++i;
                std::cout << iter->key().ToStringView() << " " << iter->value().ToStringView() << "\n";
                break;
            }

            iter->Next();
        }
    }
    std::cout << "Total keys " << i << "\n";
#endif

    for (auto & [_, cf_handle] : column_families)
    {
        status = db->DestroyColumnFamilyHandle(cf_handle);
        if (!status.ok())
            std::cerr << "Failed to destroy column family handle\n";
    }

    db->Close();

    return 0;
}
