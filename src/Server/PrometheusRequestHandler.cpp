#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{
void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event &)
{
    try
    {
        setResponseDefaultHeaders(response);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD);
        /// Write the metrics and finalize once. An earlier version also called finalize() from a
        /// nested catch, double-finalizing the buffer on aborted scrapes; rely on the outer catch
        /// below to log any failure instead.
        metrics_writer.write(wb);
        wb.finalize();
    }
    catch (...)
    {
        tryLogCurrentException("PrometheusRequestHandler");
    }
}

HTTPRequestHandlerFactoryPtr
createPrometheusHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & config_prefix)
{
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
        server, PrometheusMetricsWriter(server.config(), config_prefix + ".handler", async_metrics, server.context()));
    factory->addFiltersFromConfig(server.config(), config_prefix);
    return factory;
}

}
