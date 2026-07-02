#pragma once

#include <Interpreters/SystemLog.h>

#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <base/UUID.h>

namespace DB
{

struct DeadLetter
{
    String source_name;
    String source_description;
    std::pair<uint64_t, uint64_t> sn_range;
    std::vector<String> messages;
};

struct MaterializedViewDLQElement
{
    uint64_t node_id;
    String database;
    String view_name;
    UUID uuid;

    String error;
    std::vector<DeadLetter> dead_letters;

    Field _tp_time;

    static std::string name() { return "MaterializedViewDeadLetterQueue"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};

class MaterializedViewDeadLetterQueue : public SystemLog<MaterializedViewDLQElement>
{
    using SystemLog<MaterializedViewDLQElement>::SystemLog;

public:
    MaterializedViewDeadLetterQueue(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);
};
}
