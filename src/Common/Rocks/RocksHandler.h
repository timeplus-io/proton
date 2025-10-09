#pragma once

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <base/StringRef.h>

#include <rocksdb/db.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ROCKSDB_ERROR;
extern const int LOGICAL_ERROR;
}

class RocksHandler;
using RocksHandlerPtr = std::shared_ptr<RocksHandler>;

class Rocks;
using RocksPtr = std::shared_ptr<Rocks>;

/// Rocks is a simple wrapper of RocksDB and the column families. It owns the their lifecycles.
class Rocks final : public std::enable_shared_from_this<Rocks>
{
public:
    Rocks(rocksdb::DB * db_, const std::vector<rocksdb::ColumnFamilyHandle *> & cf_handles_, bool cleanup_, LoggerPtr logger_);
    ~Rocks();

    static RocksPtr
    createOrLoadIfExists(const rocksdb::Options & options, const std::string & path, bool cleanup_ = true, LoggerPtr logger = nullptr);

    void shutdown(bool cleanup_);

    bool isShutdown() const { return shutdown_flag.test(); }

    /// If handle_id is empty, return default handler
    RocksHandlerPtr getOrCreateHandler(const std::string & handle_id = {}, std::optional<rocksdb::ColumnFamilyOptions> cf_options = {});

    void destroy(const std::string & handle_id);

    rocksdb::DB * getDB() { return db.get(); }

private:
    friend class RocksHandler;

    std::atomic_flag shutdown_flag;
    std::unique_ptr<rocksdb::DB> db;
    const bool cleanup;
    LoggerPtr logger;

    std::mutex handles_mutex;
    std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> handles;
};
using RocksPtr = std::shared_ptr<Rocks>;

/// RocksColumnFamilyHandler is a simple wrapper of RocksDB column family handle and
/// provide put / get / remove operations for the column family.
/// It doesn't own the lifecycle of the RocksDB nor the column family underlying.
/// Actually it `borrows` the column family handler from `Rocks` object.
/// This class it not thread-safe
class RocksHandler final
{
private:
    using Slice = rocksdb::Slice;

    template <typename T>
    Slice toSlice(const T & x) const
    {
        if constexpr (std::is_standard_layout_v<T> && std::is_trivial_v<T>)
        {
            return {reinterpret_cast<const char *>(&x), sizeof(x)};
        }
        else if constexpr (
            std::is_same_v<T, std::string> || std::is_same_v<T, StringRef> || std::is_same_v<T, std::string_view>
            || std::is_same_v<T, Slice>)
        {
            return {x.data(), x.size()};
        }
        else
        {
            auto begin_offset = internal_buf.size();
            WriteBufferFromVector buffer(internal_buf, AppendModeTag{});
            writeBinary(x, buffer);
            buffer.finalize();
            chassert(buffer.count() == internal_buf.size() - begin_offset);
            return {internal_buf.data() + begin_offset, buffer.count()};
        }
    }

    template <typename T>
    void fromSlice(T & x, const Slice & slice) const
    {
        if constexpr (std::is_standard_layout_v<T> && std::is_trivial_v<T>)
        {
            if (slice.size() != sizeof(T)) [[unlikely]]
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Cannot read '{}' from slice of size {}", typeid(T).name(), slice.size());

            x = unalignedLoad<T>(slice.data());
        }
        else if constexpr (
            std::is_same_v<T, std::string> || std::is_same_v<T, StringRef> || std::is_same_v<T, std::string_view>
            || std::is_same_v<T, Slice>)
        {
            x = {slice.data(), slice.size()};
        }
        else
        {
            ReadBufferFromString buffer({slice.data(), slice.size()});
            readBinary(x, buffer);
        }
    }

public:
    explicit RocksHandler(RocksPtr rocks_, rocksdb::ColumnFamilyHandle * cf_handle_ = nullptr);

    template <typename Key, typename Value>
    void put(const Key & key, const Value & value)
    {
        internal_buf.clear();
        auto status = db->Put(write_options, cf_handle, toSlice(key), toSlice(value));
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to put key: {}", status.ToString());
    }

    template <typename Key, typename Value>
    void get(const Key & key, Value & value) const
    {
        internal_buf.clear();
        rocksdb::PinnableSlice pinnable_value(&internal_buf);
        auto status = db->Get(read_options, cf_handle, toSlice(key), &pinnable_value);
        if (status.ok())
            return fromSlice(value, pinnable_value);

        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get key: {}", status.ToString());
    }

    template <typename Key, typename Value>
    bool tryGet(const Key & key, Value & value) const
    {
        internal_buf.clear();
        rocksdb::PinnableSlice pinnable_value(&internal_buf);
        auto status = db->Get(read_options, cf_handle, toSlice(key), &pinnable_value);
        if (status.ok())
        {
            fromSlice(value, pinnable_value);
            return true;
        }
        else if (status.IsNotFound())
        {
            return false;
        }

        throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to get key: {}", status.ToString());
    }

    template <typename Key>
    void remove(const Key & key)
    {
        internal_buf.clear();
        auto status = db->Delete(write_options, cf_handle, toSlice(key));
        if (!status.ok())
            throw DB::Exception(ErrorCodes::ROCKSDB_ERROR, "Failed to remove key: {}", status.ToString());
    }

    /// Destroy current column family
    /// After executing this function, the handler and column family is invalid and should not be used anymore
    void destroy();

    ALWAYS_INLINE const std::string & getDBName() const { return db->GetName(); }
    ALWAYS_INLINE std::string_view getHandleID() const
    {
        const auto & id = cf_handle->GetName();
        return id == ROCKSDB_NAMESPACE::kDefaultColumnFamilyName ? std::string_view("") : std::string_view(id);
    }

    ALWAYS_INLINE RocksPtr getRocksHolder() const { return rocks.lock(); }

    rocksdb::DB * db;
    rocksdb::ColumnFamilyHandle * cf_handle;

private:
    /// Caller should ensure Rocks and ColumnFamilyHandle is alive
    std::weak_ptr<Rocks> rocks;

    rocksdb::WriteOptions write_options;
    rocksdb::ReadOptions read_options;

    mutable std::string internal_buf;
};
}
