#pragma once

#include <Server/RestRouterHandlers/RestRouterHandler.h>

#include <Interpreters/Context.h>

#include <memory>

namespace DB
{

/// Get metrics of different kinds of stream
class DependenciesHandler final : public RestRouterHandler
{
public:
    explicit DependenciesHandler(ContextMutablePtr query_context_) : RestRouterHandler(std::move(query_context_), "DependenciesHandler") { }
    ~DependenciesHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;

    struct Relation
    {
        String from;
        String from_type;
        String to;
        String to_type;

        Relation(String from_, String from_type_, String to_, String to_type_)
            : from(std::move(from_)), from_type(std::move(from_type_)), to(std::move(to_)), to_type(std::move(to_type_))
        {
        }

        [[nodiscard]] Poco::JSON::Object toJSON() const;
    };
    using RelationPtr = std::shared_ptr<Relation>;

    struct Dependencies
    {
        std::vector<RelationPtr> dependencies;
        std::vector<RelationPtr> data_flow;
    };
    using DependenciesPtr = std::shared_ptr<Dependencies>;

    void loadDependencies(const String & ns, const String & name) const;

    void loadStreams(const String & ns, const String & name) const;
    void loadTasks(const String & ns, const String & name) const;
    void fillStreamsType() const;

    String buildResponse(const String & filter_database, const String & filter_stream) const;

    /// Store and pass data in loading dependencies. Mark 'mutable' for executeGet() is const
    mutable DependenciesPtr dependencies;
    mutable std::unordered_map<String, String> visited_streams;  /// <fullname, type>
    mutable std::vector<StorageID> unvisited_streams;
};

}
