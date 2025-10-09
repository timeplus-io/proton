#pragma once

#include <Access/IAccessEntity.h>
#include <Access/IAccessStorage.h>

#include <Cluster/Common/Error.h>
#include <Common/SharedMutex.h>

#include <boost/container/flat_set.hpp>


namespace cluster::meta
{
class MetaStore;
}

namespace DB
{
class AccessChangesNotifier;

/// Loads and saves access entities in MetaDB. This storage is designed to store access entities consistently in the cluster.
/// - When the `IAccessStorage` write API (insertImpl/removeImpl/updateImpl) is called,
///   it will propose cluster metadata write request instead of directly writing to MetaDB.
/// - In handling the write request, MetaDataUpdater will call the local write API (insertLocal/removeLocal/updateLocal)
///   which do the actual change in MetaDB.
class MetaStoreAccessStorage final : public IAccessStorage
{
public:
    static constexpr char STORAGE_TYPE[] = "metastore";

    MetaStoreAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_);

    const char * getStorageType() const override { return STORAGE_TYPE; }

    bool exists(const UUID & id) const noexcept override;

    /// Methods called by MetadataUpdater to write/delete access entity in MetaDB
    cluster::Error
    insertLocal(const UUID & id, uint32_t entity_version, const std::string & new_entity_str, bool replace_if_exists, int64_t sn);

    cluster::Error removeLocal(const UUID & id, int64_t sn);

    cluster::Error updateLocal(const UUID & id, uint32_t version_before_update, const std::string & new_entity_str, int64_t sn);

private:
    std::optional<UUID> findImpl(AccessEntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(AccessEntityType type) const override;
    AccessEntityPtr readImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<String> readNameImpl(const UUID & id, bool throw_if_not_exists) const override;
    std::optional<UUID> insertImpl(const AccessEntityPtr & entity, bool replace_if_exists, bool throw_if_exists) override;
    bool removeImpl(const UUID & id, bool throw_if_not_exists) override;
    bool updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists) override;

    using NameToIDMap = std::unordered_map<String, UUID>;
    struct Entry
    {
        UUID id;
        String name;
        AccessEntityType type;
        AccessEntityPtr entity;
        uint32_t version{0};
    };

    AccessChangesNotifier & changes_notifier;

    mutable SharedMutex mutex;
    std::unordered_map<UUID, Entry> entries_by_id;
    std::unordered_map<std::string_view, Entry *> entries_by_name_and_type[static_cast<size_t>(AccessEntityType::MAX)];

    friend class AccessControl;
};
}
