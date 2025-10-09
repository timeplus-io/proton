#pragma once

#include <Cluster/Protocol/CreateFormatSchemaRequestData.h>
#include <Interpreters/Context_fwd.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{
/// Manages format schemas.
class FormatSchemaFactory final : private boost::noncopyable
{
public:
    /// options for what to do if a schema already exists
    using ExistsOP = cluster::protocol::ExistsOperation;

    static FormatSchemaFactory & instance();

    void registerSchema(
        const String & schema_name, const String & format, std::string_view schema_body, ExistsOP exists_op, ContextPtr & context);

    /// Unregisters the schema that match the schema_name and format.
    /// If format is empty, it will find the schema with the schema_name, and if multiple match, throws exception.
    /// Returns the format of the unregistered schemas, mostly useful when format is empty.
    String unregisterSchema(const String & schema_name, const String & format, bool throw_if_not_exists, const ContextPtr & context);

    struct SchemaEntry
    {
        String name;
        String type;
    };

    std::vector<SchemaEntry> getSchemasList(const String & format, const ContextPtr & context) const;

    struct SchemaEntryWithBody final : SchemaEntry
    {
        String body;
    };

    SchemaEntryWithBody getSchema(const String & schema_name, const String & format, const ContextPtr & context) const;

private:
    void checkSchemaType(const String & format) const;

    String findSchemaFile(const String & schema_name, const String & format, const ContextPtr & context) const;

    mutable std::recursive_mutex mutex;
};
}
