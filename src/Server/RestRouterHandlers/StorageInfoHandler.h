#pragma once

#include "RestRouterHandler.h"

#include <Interpreters/Context.h>
#include <NativeLog/Server/NativeLog.h>

namespace DB
{
struct StorageInfoForStream
{
    StorageID id = StorageID::createEmpty();
    uint64_t streaming_data_bytes = 0;
    std::vector<String> streaming_data_paths;
    uint64_t historical_data_bytes = 0;
    std::vector<String> historical_data_paths;

    Poco::Dynamic::Var toJSON(bool is_simple = false) const;
};

struct StorageInfo
{
    uint64_t total_bytes_on_disk = 0;
    std::unordered_map<String, StorageInfoForStream> streams;
    std::optional<bool> need_sort_by_bytes; /// {}: not sort, true: desc sort, false: asc sort

    Poco::Dynamic::Var toJSON(bool is_simple = false) const;
    void sortingStreamByBytes(bool desc = true) { need_sort_by_bytes = desc; }
};
using StorageInfoPtr = std::shared_ptr<StorageInfo>;

/// Get bytes on disk for streams, contains:
/// 1) data of streaming store (NativeLog)
/// 2) data of historical store (Storage)
class StorageInfoHandler final : public RestRouterHandler
{
public:
    explicit StorageInfoHandler(ContextMutablePtr query_context_)
        : RestRouterHandler(query_context_, "StorageInfoHandler"), native_log(nlog::NativeLog::instance(nullptr))
    {
    }
    ~StorageInfoHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;

private:
    StorageInfoPtr loadStorageInfo(const String & ns, const String & stream) const;
    void loadLocalStoragesInfo(StorageInfoPtr & disk_info, const String & ns, const String & stream) const;
    auto getLocalStorageInfo(StoragePtr storage) const;

    nlog::NativeLog & native_log;
};

}
