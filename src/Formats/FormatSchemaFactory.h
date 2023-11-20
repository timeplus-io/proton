#pragma once

#include <boost/core/noncopyable.hpp>
#include <Interpreters/Context_fwd.h>

#include <fstream>

namespace DB
{
/// Manages format schemas.
class FormatSchemaFactory final : private boost::noncopyable
{
public:
    /// options for what to do if a schema alread exists
    enum class ExistsOP {
        noops, /// do nothing
        replace, /// replace with the new content
        Throw, /// throw an exception
    };

    static FormatSchemaFactory & instance();

    void registerSchema(const ContextPtr & context, const String & schema_name, const String & schema_type, const String & schema_body, ExistsOP if_exists);

    void unregisterSchema(const ContextPtr & context, const String & schema_name, const String & schema_type, bool throw_if_not_exists);

    struct SchemaEntry
    {
        String name;
        String type;
    };

    std::vector<SchemaEntry> getSchemasList(const ContextPtr & context, const String & schema_type) const;

    struct SchemaEntryWithBody final: SchemaEntry
    {
        String body;
    };

    SchemaEntryWithBody getSchema(const ContextPtr & context, const String & schema_name, const String & schema_type) const;

private:
    void checkSchemaType(const String & schema_type) const;

    String findSchemaFile(const ContextPtr & context, const String & schema_name, const String & schema_type) const;

    mutable std::recursive_mutex mutex;
};
}
