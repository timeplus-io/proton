#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

#include "PrometheusMetricsWriter.h"

namespace DB
{

class IServer;

class PrometheusRequestHandler : public HTTPRequestHandler
{
private:
    const PrometheusMetricsWriter & metrics_writer;

public:
    PrometheusRequestHandler(IServer & /*server_*/, const PrometheusMetricsWriter & metrics_writer_)
        : metrics_writer(metrics_writer_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
