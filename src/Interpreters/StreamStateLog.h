#pragma once

#include <Interpreters/SystemLog.h>

#include <Cluster/Common/TimeWheel/TimerTask.h>
#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <base/UUID.h>


namespace DB
{

struct StreamStateLogElement
{
    uint64_t node_id;
    String database;
    String stream_name;
    UUID uuid;

    String state_name;
    UInt64 state_value;
    String state_string_value;
    String dimension;

    Field _tp_time;

    static std::string name() { return "StreamStateLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class StreamStateLog : public SystemLog<StreamStateLogElement>
{
    using SystemLog<StreamStateLogElement>::SystemLog;

public:
    StreamStateLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    void shutdown() override;

    void startCollectStates(int64_t collect_interval_milliseconds_);

    void stopCollectStates();

private:
    void collectStates();
    void doCollectStates();

    size_t collect_interval_milliseconds;
    std::atomic_flag stopped;

    cluster::TimerTaskEntryPtr timer_task;
};
}
