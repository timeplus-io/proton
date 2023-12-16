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
        Ignore, /// do nothing
        Replace, /// replace with the new content
        Throw, /// throw an exception
    };

    static FormatSchemaFactory & instance();

    void registerSchema(const String & schema_name, const String & schema_type, const String & schema_body, ExistsOP exists_op, const ContextPtr & context);

    void unregisterSchema(const String & schema_name, const String & schema_type, bool throw_if_not_exists, const ContextPtr & context);

    struct SchemaEntry
    {
        String name;
        String type;
    };

    std::vector<SchemaEntry> getSchemasList(const String & schema_type, const ContextPtr & context) const;

    struct SchemaEntryWithBody final: SchemaEntry
    {
        String body;
    };

    SchemaEntryWithBody getSchema(const String & schema_name, const String & schema_type, const ContextPtr & context) const;

private:
    void checkSchemaType(const String & schema_type) const;

    String findSchemaFile(const String & schema_name, const String & schema_type, const ContextPtr & context) const;

    mutable std::recursive_mutex mutex;
};
}
