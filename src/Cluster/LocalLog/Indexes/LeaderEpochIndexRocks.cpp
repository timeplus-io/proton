#include <Cluster/LocalLog/Indexes/LeaderEpochIndexRocks.h>
#include <Cluster/Rocks/IntegerComparator.h>

#include <Common/logger_useful.h>

#include <rocksdb/db.h>

namespace DB
{
namespace ErrorCodes
{
const extern int CANNOT_OPEN_DATABASE;
const extern int INSERT_KEY_VALUE_FAILED;
const extern int DELETE_KEY_VALUE_FAILED;
}
}

namespace cluster::nlog
{
LeaderEpochIndexRocks::LeaderEpochIndexRocks(const fs::path & ckpt_dir, LoggerPtr logger_) : logger(logger_)
{
    rocksdb::Options options;
    options.num_levels = 1;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.atomic_flush = true;
    options.comparator = integerComparator<uint64_t>();

    rocksdb::ColumnFamilyOptions cf_options;
    cf_options.comparator = integerComparator<uint64_t>();

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.reserve(1);
    /// default cf : epoch => sn mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName, cf_options));
    /// sn => epoch mapping
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("sn_to_epoch", cf_options));

    std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;
    cf_handles.reserve(2);

    rocksdb::DB * db = nullptr;

    auto status = rocksdb::DB::Open(options, ckpt_dir, column_families, &cf_handles, &db);
    if (!status.ok())
        throw DB::Exception(DB::ErrorCodes::CANNOT_OPEN_DATABASE, "Filed to open epoch sequence index db, {}", status.ToString());

    epoch_db.reset(db);
    epoch_sn_cf_handle = cf_handles[0];
    sn_epoch_cf_handle = cf_handles[1];

    loadLatestEpoch();
}

void LeaderEpochIndexRocks::loadLatestEpoch()
{
    /// Find latest and latest previous if we have in the database
    std::unique_ptr<rocksdb::Iterator> iter{epoch_db->NewIterator(rocksdb::ReadOptions{}, epoch_sn_cf_handle)};
    iter->SeekToLast();
    if (iter->Valid())
    {
        auto get_epoch_sn = [](const auto & it) {
            assert(it->key().size() == sizeof(uint64_t));
            auto epoch = *reinterpret_cast<const uint64_t *>(it->key().data());
            assert(it->value().size() == sizeof(int64_t));
            auto sn = *reinterpret_cast<const int64_t *>(it->value().data());

            return EpochSequence{epoch, sn};
        };

        latest_epoch_sn_cached = get_epoch_sn(iter);

        iter->Prev();
        if (iter->Valid())
        {
            previous_epoch_sn_cached = get_epoch_sn(iter);
            assert(*latest_epoch_sn_cached > *previous_epoch_sn_cached);
        }
    }
}

LeaderEpochIndexRocks::~LeaderEpochIndexRocks()
{
    if (!epoch_db)
        return;

    for (auto cf_handle : {epoch_sn_cf_handle, sn_epoch_cf_handle})
    {
        if (cf_handle != nullptr)
        {
            auto status = epoch_db->DestroyColumnFamilyHandle(cf_handle);
            if (!status.ok())
                LOG_ERROR(logger, "Failed to destroy epoch column family handle");
        }
    }

    auto status = epoch_db->Close();
    if (!status.ok())
        LOG_ERROR(logger, "Failed to close epoch db");

    LOG_INFO(logger, "Closed leader epoch checkpoint successfully");
}

bool LeaderEpochIndexRocks::maybeIndexEpochStartSequence(uint64_t epoch, int64_t sn)
{
    auto latest_epoch_sn = latestLeaderEpochSequence();
    auto need_update = !latest_epoch_sn || (latest_epoch_sn->epoch != epoch || sn < latest_epoch_sn->sn);

    if (need_update)
    {
        rocksdb::Slice epoch_s{reinterpret_cast<char *>(&epoch), sizeof(epoch)};
        rocksdb::Slice sn_s{reinterpret_cast<char *>(&sn), sizeof(sn)};

        rocksdb::WriteBatch batch;
        batch.Put(epoch_sn_cf_handle, epoch_s, sn_s);
        batch.Put(sn_epoch_cf_handle, sn_s, epoch_s);

        rocksdb::WriteOptions options;
        options.sync = true;

        auto status = epoch_db->Write(options, &batch);
        if (!status.ok())
        {
            LOG_ERROR(logger, "Failed to update epoch status index, {}", status.ToString());
            throw DB::Exception(
                DB::ErrorCodes::INSERT_KEY_VALUE_FAILED, "Filed to update epoch sequence in checkpoint, {}", status.ToString());
        }

        updateCached(epoch, sn);
    }

    return need_update;
}

std::optional<EpochSequence> LeaderEpochIndexRocks::latestLeaderEpochSequence() const
{
    if (latest_epoch_sn_cached)
        return latest_epoch_sn_cached;

    return {};
}

std::optional<EpochSequence> LeaderEpochIndexRocks::previousLeaderEpochSequence() const
{
    if (previous_epoch_sn_cached)
        return previous_epoch_sn_cached;

    return {};
}

std::optional<uint64_t> LeaderEpochIndexRocks::latestLeaderEpoch() const
{
    if (latest_epoch_sn_cached)
        return latest_epoch_sn_cached->epoch;

    return {};
}

std::optional<uint64_t> LeaderEpochIndexRocks::previousLeaderEpoch() const
{
    if (previous_epoch_sn_cached)
        return previous_epoch_sn_cached->epoch;

    return {};
}

std::optional<EpochSequence> LeaderEpochIndexRocks::leaderEpochSequenceFor(int64_t sn) const
{
    /// Empty entries or too big sn passed in
    if (!latest_epoch_sn_cached)
        return {};

    /// sn is in the current epoch
    if (sn >= latest_epoch_sn_cached->sn)
        return latest_epoch_sn_cached;

    if (previous_epoch_sn_cached && sn >= previous_epoch_sn_cached->sn)
        return previous_epoch_sn_cached;

    std::unique_ptr<rocksdb::Iterator> iter{epoch_db->NewIterator(rocksdb::ReadOptions{}, sn_epoch_cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<char *>(&sn), sizeof(sn)};
    iter->SeekForPrev(key_s);
    if (iter->Valid())
    {
        assert(iter->key().size() == sizeof(int64_t));
        auto prev_sn = *reinterpret_cast<const int64_t *>(iter->key().data());
        assert(iter->value().size() == sizeof(int64_t));
        auto pre_epoch = *reinterpret_cast<const uint64_t *>(iter->value().data());
        assert(prev_sn <= sn);

        return EpochSequence{pre_epoch, prev_sn};
    }
    return {};
}

void LeaderEpochIndexRocks::clear()
{
    rocksdb::WriteOptions options;
    options.sync = true;

    rocksdb::WriteBatch delete_batch;

    uint64_t min_key = 0;
    uint64_t max_key = std::numeric_limits<uint64_t>::max();
    rocksdb::Slice min_key_s(reinterpret_cast<char *>(&min_key), sizeof(min_key));
    rocksdb::Slice max_key_s(reinterpret_cast<char *>(&max_key), sizeof(max_key));

    delete_batch.DeleteRange(epoch_sn_cf_handle, min_key_s, max_key_s);
    delete_batch.DeleteRange(sn_epoch_cf_handle, min_key_s, max_key_s);
    auto status = epoch_db->Write(options, &delete_batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to clear epoch sequence checkpoint, error={}", status.ToString());
        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED, "Failed to clear epoch sequence checkpoint, error={}", status.ToString());
    }

    latest_epoch_sn_cached = {};
    previous_epoch_sn_cached = {};
}

void LeaderEpochIndexRocks::trim(int64_t sn)
{
    std::unique_ptr<rocksdb::Iterator> iter{epoch_db->NewIterator(rocksdb::ReadOptions{}, sn_epoch_cf_handle)};
    rocksdb::Slice key_s{reinterpret_cast<const char *>(&sn), sizeof(sn)};
    iter->Seek(key_s);
    if (!iter->Valid())
        return;

    auto max_key = std::numeric_limits<uint64_t>::max();
    auto max_key_s = rocksdb::Slice{reinterpret_cast<const char *>(&max_key), sizeof(max_key)};

    rocksdb::WriteOptions options;
    options.sync = true;

    rocksdb::WriteBatch delete_batch;

    delete_batch.DeleteRange(sn_epoch_cf_handle, iter->key(), max_key_s);
    delete_batch.DeleteRange(epoch_sn_cf_handle, iter->value(), max_key_s);

    auto status = epoch_db->Write(options, &delete_batch);
    if (!status.ok())
    {
        LOG_ERROR(logger, "Failed to trim epoch indexes to sn={} error={}", sn, status.ToString());
        throw DB::Exception(
            DB::ErrorCodes::DELETE_KEY_VALUE_FAILED,
            "Failed to trim epoch indexes to sn={} error={}",
            sn,
            status.ToString());
    }

    loadLatestEpoch();
}

void LeaderEpochIndexRocks::updateCached(uint64_t epoch, int64_t sn)
{
    if (latest_epoch_sn_cached)
    {
        if (epoch > latest_epoch_sn_cached->epoch)
            /// Update previous first
            previous_epoch_sn_cached = latest_epoch_sn_cached;

        latest_epoch_sn_cached->epoch = epoch;
        latest_epoch_sn_cached->sn = sn;
    }
    else
    {
        latest_epoch_sn_cached = EpochSequence{epoch, sn};
    }
}

}
