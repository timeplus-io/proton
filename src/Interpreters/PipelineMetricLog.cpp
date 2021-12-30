#include <base/ClockUtils.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/PipelineMetricLog.h>
#include <Common/ProtonCommon.h>


namespace DB
{

NamesAndTypesList PipelineMetricLogElement::getNamesAndTypes()
{
    return {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
        {"milliseconds", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"query", std::make_shared<DataTypeString>()},
        {"pipeline_metric", std::make_shared<DataTypeString>()}
    };
}


void PipelineMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(milliseconds);
    columns[column_idx++]->insert(query_id);
    columns[column_idx++]->insert(query);
    columns[column_idx++]->insert(pipeline_metric);
}


void PipelineMetricLog::startCollectMetric(size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown = false;
    metric_flush_thread = ThreadFromGlobalPool([this] { metricThreadFunction(); });
}


void PipelineMetricLog::stopCollectMetric()
{
    bool old_val = false;
    if (!is_shutdown.compare_exchange_strong(old_val, true))
        return;
    metric_flush_thread.join();
}


void PipelineMetricLog::shutdown()
{
    stopCollectMetric();
    stopFlushThread();
}


void PipelineMetricLog::metricThreadFunction()
{
    auto next_collect_time = std::chrono::system_clock::now();

    while (!is_shutdown)
    {
        try
        {
            const auto current_time = std::chrono::system_clock::now();
            auto context = getContext();
            ProcessList::Info info = context->getProcessList().getInfo(true, true, true);

            ///  FIXME: we don't need log pipeline metrics for all queries. We will need filter out foreground ad-hoc query and internal query.
            for (const auto & process : info)
            {
                PipelineMetricLogElement elem;

                elem.event_time = std::chrono::system_clock::to_time_t(current_time);
                elem.event_time_microseconds = UTCMicroseconds::count(current_time);
                elem.milliseconds = UTCMilliseconds::count(current_time) - UTCSeconds::count(current_time) * 1000;

                elem.query_id = process.client_info.current_query_id;
                elem.query = process.query;
                if (process.pipeline_metrics.empty())
                {
                    continue;
                }
                elem.pipeline_metric = process.pipeline_metrics;
                this->add(elem);
            }
            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (next_collect_time <= current_time)
                next_collect_time += std::chrono::milliseconds(collect_interval_milliseconds);

            std::this_thread::sleep_until(next_collect_time);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
