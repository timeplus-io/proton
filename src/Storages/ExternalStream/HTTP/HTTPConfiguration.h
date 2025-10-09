#pragma once

#include <Formats/FormatSettings.h>
#include <IO/CompressionMethod.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPHeaderEntries.h>

namespace DB
{

namespace ExternalStream
{

struct HTTPConfiguration
{
    String uri;
    String http_method;
    HTTPHeaderEntries headers;
    String ca_cert_file;
    String client_key_file;
    bool insecure_connection;
    String format;
    FormatSettings format_settings;
    CompressionMethod compression_method;
    bool use_chunked_transfer_encoding;
    ConnectionTimeouts timeouts;
};

}

}
