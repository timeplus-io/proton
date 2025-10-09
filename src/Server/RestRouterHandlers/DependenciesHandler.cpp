#include <Server/RestRouterHandlers/DependenciesHandler.h>

#include <Databases/IDatabase.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/StorageView.h>
#include <Storages/Stream/StorageStream.h>
#include <Task/Utils.h>
#include <Common/quoteString.h>

#include <boost/range/join.hpp>

#include <ranges>

namespace DB
{

namespace
{
String getFullName(const String & database, const String & stream)
{
    return fmt::format("{}.{}", backQuoteIfNeed(database), backQuoteIfNeed(stream));
}

String getFullName(const StorageID & id)
{
    return getFullName(id.getDatabaseName(), id.getTableName());
}
}

Poco::JSON::Object DependenciesHandler::Relation::toJSON() const
{
    Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);
    obj.set("from", from);
    obj.set("from_type", from_type);
    obj.set("to", to);
    obj.set("to_type", to_type);
    return obj;
}

std::pair<String, Int32> DependenciesHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & database = getPathParameter("database");
    const auto & stream = getPathParameter("stream");
    try
    {
        /// const auto access = query_context->getAccess();
        /// if (!access->isGranted(AccessType::SHOW_TABLES))
        ///     return {jsonErrorResponse("Not enough privileges", ErrorCodes::ACCESS_DENIED), HTTPResponse::HTTP_BAD_REQUEST};

        loadDependencies(database, stream);

        return {buildResponse(), HTTPResponse::HTTP_OK};
    }
    catch (...)
    {
        const auto exception = std::current_exception();
        return {jsonErrorResponse(getExceptionMessage(exception, false), getExceptionErrorCode(exception)), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

void DependenciesHandler::loadDependencies(const String & ns, const String & name) const
{
    dependencies = std::make_shared<Dependencies>();

    loadStreams(ns, name);
    loadTasks(ns, name);

    /// Load streams dependencies in other databases
    while (!unvisited_streams.empty())
    {
        std::vector<StorageID> streams_to_load;
        streams_to_load.swap(unvisited_streams);
        for (const auto & id : streams_to_load)
            loadStreams(id.database_name, id.table_name);
    }

    fillStreamsType();
}

void DependenciesHandler::loadStreams(const String & ns, const String & stream) const
{
    /// List all storages [in the specified database]
    Databases databases;
    if (!ns.empty())
        databases.emplace(ns, DatabaseCatalog::instance().getDatabase(ns));
    else
        databases = DatabaseCatalog::instance().getDatabases();

    auto get_source_table_ids = [](const IStorage * storage) -> std::vector<StorageID> {
        if (const auto storage_metadata = storage->getInMemoryMetadataPtr())
            return storage_metadata->getSelectQuery().select_table_ids;
        return {};
    };

    for (const auto & database : databases | std::views::values)
    {
        for (auto iterator = database->getTablesIterator(query_context); iterator->isValid(); iterator->next())
        {
            const auto & storage = iterator->table();

            if (!stream.empty() && storage->getStorageID().table_name != stream)
                continue;

            const auto stream_fullname = getFullName(storage->getStorageID());
            const auto stream_type = storage->getName();

            visited_streams.emplace(stream_fullname, stream_type);

            std::vector<StorageID> source_table_ids;
            std::optional<StorageID> target_table_id;

            if (const auto * storage_materialized_view = dynamic_cast<StorageMaterializedView *>(storage.get()))
            {
                source_table_ids = get_source_table_ids(storage_materialized_view);
                if (auto target_storage = storage_materialized_view->tryGetTargetTable())
                    target_table_id = target_storage->getStorageID();
            }
            else if (const auto * storage_view = dynamic_cast<StorageView *>(storage.get()))
            {
                source_table_ids = get_source_table_ids(storage_view);
            }

            for (const auto & storage_id : source_table_ids)
            {
                const auto & source_fullname = getFullName(storage_id);
                if (!visited_streams.contains(source_fullname))
                    unvisited_streams.push_back(storage_id);

                dependencies->dependencies.push_back(std::make_shared<Relation>(stream_fullname, stream_type, source_fullname, ""));
                dependencies->data_flow.push_back(std::make_shared<Relation>(source_fullname, "", stream_fullname, stream_type));
            }

            if (target_table_id.has_value() && target_table_id.value() != storage->getStorageID())
            {
                const auto & target_fullname = getFullName(target_table_id.value());
                if (!visited_streams.contains(target_fullname))
                    unvisited_streams.push_back(target_table_id.value());

                dependencies->dependencies.push_back(std::make_shared<Relation>(stream_fullname, stream_type, target_fullname, ""));
                dependencies->data_flow.push_back(std::make_shared<Relation>(stream_fullname, stream_type, target_fullname, ""));
            }
        }
    }
}

void DependenciesHandler::loadTasks(const String & ns, const String & name) const
{
    for (const auto task_infos = Task::getTaskInfos(ns); const auto & task_info : task_infos)
    {
        if (!name.empty() && task_info.id.table_name != name)
            continue;

        auto task_fullname = getFullName(task_info.id);

        if (task_info.target_table.has_value())
        {
            const auto target_table_fullname = getFullName(task_info.target_table.value());
            if (!visited_streams.contains(target_table_fullname))
                unvisited_streams.push_back(task_info.target_table.value());

            dependencies->dependencies.push_back(std::make_shared<Relation>(task_fullname, "Task", target_table_fullname, ""));
            dependencies->data_flow.push_back(std::make_shared<Relation>(task_fullname, "Task", target_table_fullname, ""));
        }

        for (const auto & source_id : task_info.source_tables)
        {
            auto source_table_fullname = getFullName(source_id);
            if (!visited_streams.contains(source_table_fullname))
                unvisited_streams.push_back(source_id);

            dependencies->dependencies.push_back(std::make_shared<Relation>(task_fullname, "Task", source_table_fullname, ""));
            dependencies->data_flow.push_back(std::make_shared<Relation>(source_table_fullname, "", task_fullname, "Task"));
        }
    }
}

void DependenciesHandler::fillStreamsType() const
{
    auto setType = [this](const String & stream_name, String & type) -> void {
        if (type.empty())
        {
            if (const auto iter = visited_streams.find(stream_name); iter != visited_streams.end())
                type = iter->second;
            else
                type = "Unknown";
        }
    };

    for (const auto & rel : boost::join(dependencies->dependencies, dependencies->data_flow))
    {
        setType(rel->from, rel->from_type);
        setType(rel->to, rel->to_type);
    }
}

String DependenciesHandler::buildResponse() const
{
    /// @FORMAT:
    /// {
    ///     "request_id": xxx,
    ///     "data":
    ///     {
    ///         "dependencies":
    ///         [
    ///             {
    ///                 "from": "stream-1",
    ///                 "from_type": "MaterializedView",
    ///                 "to": "stream-2",
    ///                 "to_type": "Stream"
    ///             },
    ///             ...
    ///         ],
    ///         "data_flow":
    ///         [
    ///             {
    ///                 ...
    ///             },
    ///             ...
    ///         ]
    ///     }
    /// }

    const String & query_id = query_context->getCurrentQueryId();
    Poco::JSON::Object resp(Poco::JSON_PRESERVE_KEY_ORDER);
    resp.set("request_id", query_id);

    if (dependencies)
    {
        Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);

        Poco::JSON::Array dependencies_info;
        for (const auto & dependency : dependencies->dependencies)
            dependencies_info.add(dependency->toJSON());
        obj.set("dependencies", dependencies_info);

        Poco::JSON::Array data_flow_info;
        for (const auto & flow : dependencies->data_flow)
            data_flow_info.add(flow->toJSON());
        obj.set("data_flow", data_flow_info);

        resp.set("data", obj);
    }

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

}
