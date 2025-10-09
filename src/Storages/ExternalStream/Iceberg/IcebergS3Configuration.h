#pragma once

#include "config.h"

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3Common.h>
#include <Storages/StorageConfiguration.h>
#include <Storages/StorageS3Settings.h>

namespace DB
{

namespace ExternalStream
{

struct IcebergS3Configuration : public StatelessTableEngineConfiguration
{
    DB::S3::URI url;
    std::shared_ptr<const DB::S3::Client> client;
    DB::S3::AuthSettings auth_settings;
    S3Settings::RequestSettings request_settings;
    /// If s3 configuration was passed from ast, then it is static.
    /// If from config - it can be changed with config reload.
    bool static_configuration = true;
    /// Headers from ast is a part of static configuration.
    HTTPHeaderEntries headers_from_ast;

    /// proton: starts
    UInt64 min_upload_file_size = 0;
    UInt64 max_upload_idle_seconds = 0;
    /// proton: ends
    ///
public:
    void createClient(const ContextPtr & ctx);
};

}

}

#endif
