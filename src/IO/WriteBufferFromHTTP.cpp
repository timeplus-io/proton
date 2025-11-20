#include <IO/WriteBufferFromHTTP.h>

#include <Common/logger_useful.h>


namespace DB
{

WriteBufferFromHTTP::WriteBufferFromHTTP(
    const Poco::URI & uri,
    const std::string & method,
    const std::string & content_type,
    const std::string & content_encoding,
    const ConnectionTimeouts & timeouts,
    size_t buffer_size_)
    : WriteBufferFromOStream(buffer_size_)
    , session(makeHTTPSession(uri, timeouts))
    , request{method, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1}
{
    init(uri, content_type, content_encoding, /*additional_headers=*/{}); /// proton: updated
}

WriteBufferFromHTTP::WriteBufferFromHTTP(
    PooledHTTPSessionPtr session_,
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

    ostr = std::visit([this](auto & sess) -> std::ostream * { return &sess->sendRequest(request); }, session);
}

void WriteBufferFromHTTP::finalizeImpl()
{
    // Make sure the content in the buffer has been flushed
    this->next();

    std::visit(
        [this](auto & sess) {
            auto resp = receiveResponse(*sess, request, response, false);

            if (response_body_handler)
                response_body_handler(resp);
            else
                /// For keep-alive connections, if we don't finish reading the response, it could make the next response parsing failed
                resp->ignore(std::numeric_limits<std::streamsize>::max());
        },
        session); /// proton: updated
}

void WriteBufferFromHTTP::withResponseBodyHandler(std::function<void(std::istream *)> handler)
{
    response_body_handler = handler;
}

}
