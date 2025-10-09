#include <Access/MetaStoreAccessStorage.h>

#include <Access/AccessChangesNotifier.h>
#include <Access/AccessEntityIO.h>
#include <Bootstrap/Globals.h>
#include <Cluster/Common/AppliedSequence.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/AccessEntityDescription.h>
#include <Cluster/Requests/DeleteAccessEntityRequest.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <magic_enum.hpp>

#include <optional>


namespace DB
{
namespace ErrorCodes
{
extern const int ACCESS_ENTITY_ALREADY_EXISTS;
extern const int ACCESS_ENTITY_NOT_FOUND;
extern const int METADATA_VERSION_CHANGED;
extern const int ACCESS_STORAGE_READONLY;
}

MetaStoreAccessStorage::MetaStoreAccessStorage(const String & storage_name_, AccessChangesNotifier & changes_notifier_)
    : IAccessStorage(storage_name_), changes_notifier(changes_notifier_)
{
    /// Load access entities from MetaDB to memory
    auto & metastore = Globals::getMetaStore();
    auto & db = metastore.getMetaDB();
    auto maybe_access_entities = db.listAccessEntities();
    if (maybe_access_entities.hasError())
        LOG_ERROR(getLogger(), "Error in listing access entities: {}", maybe_access_entities.err.string());

    for (const auto & entity_desc : maybe_access_entities.result)
    {
        auto id = entity_desc->id;
        auto entity = deserializeAccessEntity(entity_desc->entity, "metastore");
        auto type = entity->getType();
        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.type = type;
        entry.name = entity->getName();
        entry.entity = entity;
        entry.version = entity_desc->data_version;
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name[entry.name] = &entry;
    }
}

cluster::Error MetaStoreAccessStorage::insertLocal(
    const UUID & id, uint32_t entity_version, const std::string & new_entity_str, bool replace_if_exists, int64_t sn)
{
    AccessEntityPtr new_entity;
    try
    {
        new_entity = deserializeAccessEntity(new_entity_str, "metastore");
    }
    catch (const Exception & ex)
    {
        return {ex.code(), ex.message()};
    }

    LOG_INFO(getLogger(), "Insert access entity: type={} name={}", magic_enum::enum_name(new_entity->getType()), new_entity->getName());

    {
        std::unique_lock lock{mutex};

        const String & name = new_entity->getName();
        AccessEntityType type = new_entity->getType();

        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        /// Check name collision.
        auto it_by_name = entries_by_name.find(name);
        bool name_collision = (it_by_name != entries_by_name.end());
        if (name_collision && !replace_if_exists)
        {
            auto entity_type_with_name = formatEntityTypeWithName(type, name);
            return {
                ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS,
                fmt::format("{}: cannot insert because {} already exists in metastore", entity_type_with_name, entity_type_with_name)};
        }

        /// Check ID collision.
        auto it_by_id = entries_by_id.find(id);
        if (it_by_id != entries_by_id.end())
        {
            const auto & existing_entry = it_by_id->second;
            return {
                ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS,
                fmt::format(
                    "{}: cannot insert because the ID({}) is already used by {} in metastore",
                    formatEntityTypeWithName(type, name),
                    toString(id),
                    formatEntityTypeWithName(existing_entry.type, existing_entry.name))};
        }

        /// Save/replace access entity in MetaDB.
        cluster::protocol::AccessEntityDescription desc{id, entity_version, serializeAccessEntity(*new_entity)};
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = name_collision ? db.replaceAccessEntity(desc, it_by_name->second->id, cluster::AppliedSequence(sn))
                                  : db.saveAccessEntity(desc, cluster::AppliedSequence(sn));
        if (res.hasError())
            return res;

        if (name_collision)
        {
            /// Remove old access entity.
            UUID removed_id = it_by_name->second->id;
            entries_by_name.erase(it_by_name);
            entries_by_id.erase(removed_id);

            changes_notifier.onEntityRemoved(removed_id, type);
        }

        /// Do insertion.
        auto & entry = entries_by_id[id];
        entry.id = id;
        entry.type = type;
        entry.name = name;
        entry.entity = new_entity;
        entry.version = entity_version;
        entries_by_name[entry.name] = &entry;

        changes_notifier.onEntityAdded(id, new_entity);
    }

    changes_notifier.sendNotifications();

    return {};
}

cluster::Error MetaStoreAccessStorage::removeLocal(const UUID & id, int64_t sn)
{
    {
        std::unique_lock lock{mutex};

        auto it = entries_by_id.find(id);
        if (it == entries_by_id.end())
            return {ErrorCodes::ACCESS_ENTITY_NOT_FOUND, fmt::format("ID({}) not found in metastore", toString(id))};

        LOG_INFO(
            getLogger(),
            "Remove access entity: type={} name={} id={}",
            magic_enum::enum_name(it->second.type),
            it->second.name,
            toString(it->second.id));

        /// Delete access entity from MetaDB.
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = db.deleteAccessEntity(id, cluster::AppliedSequence(sn));
        if (res.hasError())
            return res;

        /// Do removing.
        Entry & entry = it->second;
        AccessEntityType type = entry.type;

        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
        entries_by_name.erase(entry.name);
        entries_by_id.erase(it);

        changes_notifier.onEntityRemoved(id, type);
    }

    changes_notifier.sendNotifications();

    return {};
}

cluster::Error
MetaStoreAccessStorage::updateLocal(const UUID & id, uint32_t version_before_update, const std::string & new_entity_str, int64_t sn)
{
    AccessEntityPtr new_entity;
    try
    {
        new_entity = deserializeAccessEntity(new_entity_str, "metastore");
    }
    catch (const Exception & ex)
    {
        return {ex.code(), ex.message()};
    }

    LOG_INFO(
        getLogger(),
        "Update access entity: type={} name={} id={}",
        magic_enum::enum_name(new_entity->getType()),
        new_entity->getName(),
        toString(id));

    {
        std::unique_lock lock{mutex};

        /// Check if old access entity is changed.
        auto it = entries_by_id.find(id);
        if (it == entries_by_id.end())
            return {ErrorCodes::ACCESS_ENTITY_NOT_FOUND, fmt::format("ID({}) not found in metastore", toString(id))};

        Entry & entry = it->second;
        if (version_before_update != cluster::Nulls::NullVersion && entry.version != version_before_update)
            return {ErrorCodes::METADATA_VERSION_CHANGED, "Access entity was changed before update"};

        const String & new_name = new_entity->getName();
        const String & old_name = entry.entity->getName();
        const AccessEntityType type = entry.type;
        auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];

        bool name_changed = (new_name != old_name);
        if (name_changed && entries_by_name.contains(new_name))
            return {ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS, fmt::format("{} already exists in metastore", new_name)};

        /// Update access entity in MetaDB.
        uint32_t updated_version = version_before_update + 1;
        cluster::protocol::AccessEntityDescription desc{id, updated_version, serializeAccessEntity(*new_entity)};
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = db.saveAccessEntity(desc, cluster::AppliedSequence(sn));
        if (res.hasError())
            return res;

        entry.entity = new_entity;
        entry.version = updated_version;

        if (name_changed)
        {
            entries_by_name.erase(entry.name);
            entry.name = new_name;
            entries_by_name[entry.name] = &entry;
        }

        changes_notifier.onEntityUpdated(id, new_entity);
    }

    changes_notifier.sendNotifications();

    return {};
}

std::optional<UUID> MetaStoreAccessStorage::findImpl(AccessEntityType type, const String & name) const
{
    std::shared_lock lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    auto it = entries_by_name.find(name);
    if (it == entries_by_name.end())
        return {};

    return it->second->id;
}

std::vector<UUID> MetaStoreAccessStorage::findAllImpl(AccessEntityType type) const
{
    std::shared_lock lock{mutex};
    const auto & entries_by_name = entries_by_name_and_type[static_cast<size_t>(type)];
    std::vector<UUID> res;
    res.reserve(entries_by_name.size());
    for (const auto & name_entry : entries_by_name)
        res.emplace_back(name_entry.second->id);
    return res;
}

bool MetaStoreAccessStorage::exists(const UUID & id) const noexcept
{
    std::shared_lock lock{mutex};
    return entries_by_id.contains(id);
}

AccessEntityPtr MetaStoreAccessStorage::readImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::shared_lock lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return nullptr;
    }

    return it->second.entity;
}

std::optional<String> MetaStoreAccessStorage::readNameImpl(const UUID & id, bool throw_if_not_exists) const
{
    std::shared_lock lock{mutex};
    auto it = entries_by_id.find(id);
    if (it == entries_by_id.end())
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return std::nullopt;
    }
    return it->second.name;
}

std::optional<UUID> MetaStoreAccessStorage::insertImpl(const AccessEntityPtr & new_entity, bool replace_if_exists, bool throw_if_exists)
{
    auto & metastore = Globals::getMetaStore();

    UUID id = generateRandomID();
    auto req = std::make_shared<cluster::CreateAccessEntityRequest>(
        cluster::protocol::AccessEntityDescription{id, /*version_=*/1, serializeAccessEntity(*new_entity)},
        replace_if_exists,
        0,
        /*timeout_ms_=*/10'000,
        /*request_version_=*/2);
    auto resp = metastore.createAccessEntity(std::move(req));
    const auto & res = resp->error();
    /// proton: ends
    if (!res.hasError())
        return id;

    if (res.error_code == ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS)
    {
        if (throw_if_exists)
            throwNameCollisionCannotInsert(new_entity->getType(), new_entity->getName());
        else
            return std::nullopt;
    }

    throw Exception(
        res.error_code, "Cannot insert {}: {}", formatEntityTypeWithName(new_entity->getType(), new_entity->getName()), res.string());
}

bool MetaStoreAccessStorage::removeImpl(const UUID & id, bool throw_if_not_exists)
{
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::DeleteAccessEntityRequest>(id, 0, /*timeout_ms_=*/10'000, /*request_version_=*/2);
    auto resp = metastore.deleteAccessEntity(std::move(req));
    const auto & res = resp->error();
    /// proton: ends

    if (!res.hasError())
        return true;

    if (res.error_code == ErrorCodes::ACCESS_ENTITY_NOT_FOUND)
    {
        if (throw_if_not_exists)
            throwNotFound(id);
        else
            return false;
    }

    throw Exception(res.error_code, "Cannot remove ID({}): {}", toString(id), res.string());
}

bool MetaStoreAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func, bool throw_if_not_exists)
{
    AccessEntityPtr old_entity;
    AccessEntityPtr new_entity;

    uint32_t entity_version = cluster::Nulls::NullVersion;
    {
        /// Generate the updated access entity and do the initial local check.
        std::shared_lock lock{mutex};

        auto it = entries_by_id.find(id);
        if (it == entries_by_id.end())
        {
            if (throw_if_not_exists)
                throwNotFound(id);
            else
                return false;
        }

        Entry & entry = it->second;
        old_entity = entry.entity;
        new_entity = update_func(old_entity);

        if (!new_entity->isTypeOf(old_entity->getType()))
            throwBadCast(id, new_entity->getType(), new_entity->getName(), old_entity->getType());

        if (*new_entity == *old_entity)
            return true;

        entity_version = it->second.version;
    }

    auto & metastore = Globals::getMetaStore();

    assert(entity_version != cluster::Nulls::NullVersion);

    auto req = std::make_shared<cluster::UpdateAccessEntityRequest>(
        entity_version,
        cluster::protocol::AccessEntityDescription{id, entity_version, serializeAccessEntity(*new_entity)},
        0,
        /*timeout_ms_=*/10'000,
        /*request_version_=*/2);
    auto resp = metastore.updateAccessEntity(std::move(req));
    const auto & res = resp->error();

    if (res.error_code == ErrorCodes::ACCESS_ENTITY_ALREADY_EXISTS)
        throwNameCollisionCannotRename(old_entity->getType(), old_entity->getName(), new_entity->getName());

    if (res.error_code == ErrorCodes::METADATA_VERSION_CHANGED)
        throw Exception(ErrorCodes::METADATA_VERSION_CHANGED, "Access entity is changed before update");

    if (res.hasError())
        throw Exception(
            res.error_code, "Cannot update {}: {}", formatEntityTypeWithName(old_entity->getType(), old_entity->getName()), res.string());

    return true;
}

}
