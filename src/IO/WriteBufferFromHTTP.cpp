#include <IO/WriteBufferFromHTTP.h>

#include <Common/logger_useful.h>


namespace DB
{

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const HTTPConnectionGroupType & connection_group,
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_,
    ProxyConfiguration proxy_configuration
)
    : WriteBufferFromOStream(buffer_size_)
    , session{makeHTTPSession(connection_group, uri, timeouts, proxy_configuration)}
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    init(uri, content_type, content_encoding, additional_headers); /// proton: updated
}

WriteBufferFromHTTP::WriteBufferFromHTTP(
    HTTPSessionPtr session_,
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers,
    size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , session(std::move(session_))
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    init(uri, content_type, content_encoding, additional_headers);
}

void WriteBufferFromHTTP::init(
    const Poco::URI & uri,
    const std::string & content_type,
    const std::string & content_encoding,
    const HTTPHeaderEntries & additional_headers)
{
    request.setHost(uri.getHost());

    bool has_content_length{false};
    for (const auto & header : additional_headers)
    {
        if (header.name == "Content-Length")
            has_content_length = true;
        request.add(header.name, header.value);
    }

    if (!has_content_length)
        request.setChunkedTransferEncoding(true);

    if (!content_type.empty() && !request.has(Poco::Net::HTTPMessage::CONTENT_LENGTH))
        request.setContentType(content_type);

    if (!content_encoding.empty())
        request.set("Content-Encoding", content_encoding);

    LOG_TRACE((getLogger("WriteBufferToHTTP")), "Sending request to {}", uri.toString());

    ostr = &session->sendRequest(request);
}

void WriteBufferFromHTTP::finalizeImpl()
{
    // Make sure the content in the buffer has been flushed
    this->next();

    /// proton: starts
    auto resp = receiveResponse(*session, request, response, false);

    if (response_body_handler)
        response_body_handler(resp);
    else
        /// For keep-alive connections, if we don't finish reading the response, it could make the next response parsing failed
        resp->ignore(std::numeric_limits<std::streamsize>::max());
    /// proton: ends

    WriteBufferFromOStream::finalizeImpl();
}

/// proton: starts
void WriteBufferFromHTTP::withResponseBodyHandler(std::function<void(std::istream *)> handler)
{
    response_body_handler = handler;
}
/// proton: ends

}
