#pragma once

#include <Interpreters/SystemLog.h>

#include <Cluster/Common/TimeWheel/TimerTask.h>
#include <Core/Field.h>
#include <Core/NamesAndAliases.h>
#include <base/UUID.h>


namespace DB
{

struct StreamMetricLogElement
{
    int64_t elapsed_ms;
    uint64_t node_id;

    String database;
    String stream_name;
    UUID uuid;
    String type;

    UInt64 read_bytes;
    UInt64 read_rows;
    UInt64 written_bytes;
    UInt64 written_rows;
    bool external_ingress;

    Field _tp_time;

    static std::string name() { return "StreamMetricLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class StreamMetricLog : public SystemLog<StreamMetricLogElement>
{
    using SystemLog<StreamMetricLogElement>::SystemLog;

public:
    StreamMetricLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    void shutdown() override;

    void startCollectMetrics(int64_t collect_interval_milliseconds_);
    void stopCollectMetrics();

private:
    void collectMetrics();
    void doCollectMetrics();

    size_t collect_interval_milliseconds;
    std::atomic_flag stopped;

    cluster::TimerTaskEntryPtr timer_task;
};
}
