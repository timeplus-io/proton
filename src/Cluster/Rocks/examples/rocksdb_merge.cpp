#include <rocksdb/db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>

#include <format>
#include <vector>
#include <iostream>

namespace
{
class MultipleVersionsMergeOperator final : public rocksdb::MergeOperator
{
public:
    explicit MultipleVersionsMergeOperator(size_t keep_version_) : keep_versions(keep_version_) { }

    static const char * Type() { return "MultipleVersionsMergeOperator"; }

    const char * Name() const override { return Type(); }

    rocksdb::Slice truncateExistingValue(const rocksdb::Slice & value, size_t new_ops) const noexcept
    {
        std::vector<std::string_view::size_type> truncation_positions;
        truncation_positions.reserve(keep_versions - 1);

        auto view = value.ToStringView();
        std::string_view::size_type n = 0;
        while (true)
        {
            if (n = view.find(sep, n); n != std::string_view::npos)
            {
                n += sep.size();
                truncation_positions.push_back(n);
            }
            else
            {
                break;
            }
        }

        if (truncation_positions.size() + new_ops + 1 > keep_versions)
        {
            /// Truncate
            auto pos = truncation_positions[truncation_positions.size() + new_ops - keep_versions];
            auto copy = value;
            copy.remove_prefix(pos);
            return copy;
        }
        else
        {
            return value;
        }
    }

    bool FullMergeV2(const MergeOperationInput & merge_in, MergeOperationOutput * merge_out) const override
    {
        auto num_new_ops = merge_in.operand_list.size();

        auto do_merge = [&, this](std::string_view init_value, size_t start_pos) {
            auto iter = merge_in.operand_list.begin() + start_pos;
            auto iter_end = merge_in.operand_list.end();

            size_t total_size = 0;
            for (auto iter_begin = iter; iter_begin != iter_end; ++iter_begin)
                total_size += iter_begin->size() + sep.size();

            std::string merged;
            merged.reserve(total_size + init_value.size());

            merged += init_value;

            if (!init_value.empty())
                merged += sep;

            for (auto iter_begin = iter;;)
            {
                merged += iter_begin->ToStringView();
                if (++iter_begin != iter_end)
                    merged += sep;
                else
                    break;
            }

            merge_out->new_value.swap(merged);
        };

        if (merge_in.existing_value)
        {
            if (num_new_ops >= keep_versions)
            {
                do_merge(std::string_view{}, num_new_ops - keep_versions);
            }
            else
            {
                auto truncated = truncateExistingValue(*merge_in.existing_value, num_new_ops);
                do_merge(truncated.ToStringView(), 0);
            }
        }
        else
        {
            if (num_new_ops >= keep_versions)
                do_merge(std::string_view{}, num_new_ops - keep_versions);
            else
                /// Merge
                do_merge(std::string_view{}, 0);
        }

        return true;
    }

private:
    size_t keep_versions;
    std::string sep = "`@&`";
};
}

int main()
{
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.compression = rocksdb::CompressionType::kLZ4Compression;
    options.merge_operator.reset(new MultipleVersionsMergeOperator(3));

    rocksdb::DBOptions db_options(options);
    rocksdb::DB * db = nullptr;

    if (auto status = rocksdb::DB::Open(options, "/tmp/merge", &db); status != rocksdb::Status::OK())
    {
        std::cerr << std::format("Failed to list column family : {}\n", status.ToString());
        return -1;
    }

    std::unique_ptr<rocksdb::DB> dbp{db};

    /// Base version
    if (auto status = dbp->Put(rocksdb::WriteOptions{}, "key", std::format("value_{}", 0)); !status.ok())
    {
        std::cerr << std::format("Failed to write key=value_{}, error={}\n", 0, status.ToString());
        return -1;
    }

    for (size_t i = 1; i < 30; ++i)
    {
        if (auto status = dbp->Merge(rocksdb::WriteOptions{}, "key", std::format("value_{}", i)); !status.ok())
        {
            std::cerr << std::format("Failed to write key=value_{}, error={}\n", i, status.ToString());
            return -1;
        }

        std::string value;
        if (auto status = dbp->Get(rocksdb::ReadOptions{}, "key", &value); !status.ok())
        {
            std::cerr << std::format("Failed to read key, error={}\n", status.ToString());
            return -1;
        }

        std::cout << value << "\n";
    }

    return 0;
}
