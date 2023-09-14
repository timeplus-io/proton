#include "TelemetryCollector.h"
#include "config_version.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPSClientSession.h>
#include <base/ClockUtils.h>
#include <base/getMemoryAmount.h>
#include <Common/DateLUT.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>

namespace DB
{

TelemetryCollector::TelemetryCollector(ContextPtr context_)
    : log(&Poco::Logger::get("TelemetryCollector")),
    pool(context_->getSchedulePool()),
    started_on_in_minutes(UTCMinutes::now())
{
    const auto & config = context_->getConfigRef();

    if (!config.getBool("telemetry_enabled", true))
    {
        LOG_WARNING(log, "Please note that telemetry is disabled.");
        is_shutdown.test_and_set();
        return;
    }

    WriteBufferFromOwnString wb;
    writeDateTimeTextISO(UTCMilliseconds::now(), 3, wb, DateLUT::instance("UTC"));
    started_on = wb.str();

    LOG_WARNING(log, "Please note that telemetry is enabled. "
            "This is used to collect the version and runtime environment information to Timeplus, Inc. "
            "You can disable it by setting telemetry_enabled to false in config.yaml");

    collector_task = pool.createTask("TelemetryCollector", [this]() { this->collect(); });
    collector_task->activate();
    collector_task->schedule();
}

TelemetryCollector::~TelemetryCollector()
{
    shutdown();
}

void TelemetryCollector::shutdown()
{
    if (!is_shutdown.test_and_set())
    {
        LOG_INFO(log, "Stopped");
        collector_task->deactivate();
    }
}

void TelemetryCollector::collect()
{
    SCOPE_EXIT({
        collector_task->scheduleAfter(INTERVAL_MS);
    });

    constexpr auto jitsu_url = "https://data.timeplus.com/api/s/s2s/track";
    constexpr auto jitsu_token = "U7qmIGzuZvvkp16iPaYLeBR4IHfKBY6P:Cc6EUDRmEHG9TCO7DX8x23xWrdFg8pBU";

    try
    {
        Poco::URI uri(jitsu_url);
        Poco::Net::HTTPSClientSession session(uri.getHost(), uri.getPort());
        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, uri.getPathAndQuery());

        auto memory_in_gb = getMemoryAmount() / 1024 / 1024 / 1024;
        auto cpu = getNumberOfPhysicalCPUCores();

        Int64 duration_in_minute = UTCMinutes::now() - started_on_in_minutes;

        std::string data = fmt::format("{{"
            "\"type\": \"track\","
            "\"event\": \"protonPing\","
            "\"properties\": {{"
            "    \"cpu\": \"{}\","
            "    \"memory_in_GB\": \"{}\","
            "    \"edition\": \"{}\","
            "    \"version\": \"{}\","
            "    \"new_session\": \"{}\","
            "    \"started_on\": \"{}\","
            "    \"duration_in_minute\": \"{}\""
            "}}"
        "}}", cpu, memory_in_gb, EDITION, VERSION_STRING, new_session, started_on, duration_in_minute);

        new_session = false;

        request.setContentLength(data.length());
        request.setContentType("application/json");
        request.add("X-Write-Key", jitsu_token);

        auto & requestStream = session.sendRequest(request);
        requestStream << data;

        Poco::Net::HTTPResponse response;

        auto & responseStream = session.receiveResponse(response);

        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        {
            std::stringstream ss;
            ss << responseStream.rdbuf();
            LOG_WARNING(log, "Failed to send telemetry: {}.", ss.str());
            return;
        }

        LOG_INFO(log, "Telemetry sent successfully.");
    }
    catch (Poco::Exception & ex)
    {
        LOG_WARNING(log, "Failed to send telemetry: {}.", ex.displayText());
    }
}

}
