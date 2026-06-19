#include <Storages/Iceberg/RestCatalog.h>

#if USE_AVRO
#include <Core/Settings.h>
#include <base/find_symbols.h>
#include <Common/Base64.h>
#include <Common/checkStackSize.h>
#include <Common/escapeForFileName.h>
#include <Common/sendRequest.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/S3/Client.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>

#include <Storages/Iceberg/IcebergMetadata.h>
#include <Storages/Iceberg/Schema.h>
#include <Storages/Iceberg/SchemaProcessor.h>
#include <Storages/Iceberg/StorageCredentials.h>

#include <Formats/FormatFactory.h>
#include <Server/HTTP/HTMLForm.h>

#include <aws/core/auth/signer/AWSAuthSignerHelper.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/URI.h>

namespace DB::ErrorCodes
{
extern const int AWS_ERROR;
extern const int AUTHENTICATION_FAILED;
extern const int BAD_ARGUMENTS;
extern const int ICEBERG_CATALOG_ERROR;
extern const int LOGICAL_ERROR;
}

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace Apache::Iceberg
{

static constexpr auto CONFIG_ENDPOINT = "config";
static constexpr auto NAMESPACES_ENDPOINT = "namespaces";

namespace
{

std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
pickCredentialsProvider(const String & region, const String & access_key_id, const String & secret_key, bool use_environment_credentials)
{
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> provider;

    auto client_config = DB::S3::ClientFactory::instance().createClientConfiguration(
        /*force_region=*/region,
        /*remote_host_filter=*/{},
        /*s3_max_redirects=*/10,
        /*enable_s3_requests_logging=*/false,
        /*for_disk_s3=*/false,
        /*get_request_throttler=*/nullptr,
        /*put_request_throttler=*/nullptr);

    DB::S3::S3CredentialsProviderChain provider_chain{
        /*configuration=*/client_config,
        /*credentials=*/{access_key_id, secret_key},
        /*credentials_configuration=*/{.use_environment_credentials = use_environment_credentials}};

    for (size_t i = 0; const auto & cp : provider_chain.GetProviders())
    {
        try
        {
            const auto creds = cp->GetAWSCredentials();
            if (!creds.IsEmpty())
            {
                LOG_INFO(getLogger("AWS_MSK_IAM"), "The {}th provider from the provider chain has been picked", i);
                provider = cp;
                break;
            }
            ++i;
        }
        catch (...)
        {
            /// Just skip to the next one
        }
    }

    if (!provider)
        throw DB::Exception(DB::ErrorCodes::AWS_ERROR, "No AWS credentials available");

    return provider;
}

std::pair<std::string, std::string> parseCatalogCredential(const std::string & catalog_credential)
{
    /// Parse a string of format "<client_id>:<client_secret>"
    /// into separare strings client_id and client_secret.

    std::string client_id;
    std::string client_secret;
    if (!catalog_credential.empty())
    {
        auto pos = catalog_credential.find(':');
        if (pos == std::string::npos)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Unexpected format of catalog credential: "
                "expected client_id and client_secret separated by `:`");
        }
        client_id = catalog_credential.substr(0, pos);
        client_secret = catalog_credential.substr(pos + 1);
    }
    return std::pair(client_id, client_secret);
}

DB::HTTPHeaderEntry parseAuthHeader(const std::string & auth_header)
{
    /// Parse a string of format "Authorization: <auth_scheme> <auth_token>"
    /// into a key-value header "Authorization", "<auth_scheme> <auth_token>"

    auto pos = auth_header.find(':');
    if (pos == std::string::npos)
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unexpected format of auth header");

    return DB::HTTPHeaderEntry(auth_header.substr(0, pos), auth_header.substr(pos + 1));
}

std::string correctAPIURI(const std::string & uri)
{
    if (uri.ends_with("v1"))
        return uri;
    return std::filesystem::path(uri) / "v1";
}

std::string encodeNamespaceName(const std::string & namespace_name)
{
    std::string result;
    result.reserve(namespace_name.size());

    for (auto ch : namespace_name)
    {
        if (ch != '.')
            result.push_back(ch);
        else
            /// 0x1F is a unit separator
            /// https://github.com/apache/iceberg/blob/70d87f1750627b14b3b25a0216a97db86a786992/open-api/rest-catalog-open-api.yaml#L264
            result += "%1F";
    }
    return result;
}

}

std::string RestCatalog::Config::toString() const
{
    DB::WriteBufferFromOwnString wb;

    if (!prefix.empty())
        wb << "prefix: " << prefix.string() << ", ";

    if (!default_base_location.empty())
        wb << "default_base_location: " << default_base_location << ", ";

    return wb.str();
}

RestCatalog::RestCatalog(const std::string & warehouse_, const std::string & base_url_, const Options & options, DB::ContextPtr context_)
    : ICatalog(warehouse_)
    , DB::WithContext(context_)
    , base_url(correctAPIURI(base_url_))
    , log(getLogger("RestCatalog(" + warehouse_ + ")"))
    , rest_auth_type(options.auth_type)
    , gcp_project_id(options.gcp_project_id)
    , auth_scope(options.auth_scope)
    , oauth_server_uri(options.oauth_server_uri)
    , pool(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1)
{
    [[maybe_unused]] bool use_credential{false};
    if (!options.catalog_credential.empty())
    {
        std::tie(client_id, client_secret) = parseCatalogCredential(options.catalog_credential);
        update_token_if_expired = true;
        use_credential = true;
    }

    [[maybe_unused]] bool use_header{false};
    if (!options.auth_header.empty())
    {
        assert(!use_credential);

        auth_header = parseAuthHeader(options.auth_header);
        use_header = true;
    }

    if (options.enable_sigv4)
    {
        assert(!use_header);

        aws_credentials = pickCredentialsProvider(options.signing_region, client_id, client_secret, options.use_environment_credentials);
        aws_auth_signer = std::make_shared<const Aws::Client::AWSAuthV4Signer>(
            aws_credentials,
            options.signing_name.data(),
            options.signing_region,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::RequestDependent,
            /// When `urlEscapePath` set to true, it will perform a double-escape, this will cause problem for S3 table catalog,
            /// because its prefix already has escaped characters (then it will be a tripple-escape).
            /// No impact on other REST catalog services has been identified so far.
            /*urlEscapePath=*/false);
    }

    config = loadConfig();
}

RestCatalog::Config RestCatalog::loadConfig()
{
    Poco::URI::QueryParameters params = {{"warehouse", warehouse}};
    auto buf = createReadBuffer(CONFIG_ENDPOINT, Poco::Net::HTTPRequest::HTTP_GET, params);

    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    LOG_TEST(log, "Received catalog configuration settings: {}", json_str);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    Config result;

    auto defaults_object = object->get("defaults").extract<Poco::JSON::Object::Ptr>();
    parseCatalogConfigurationSettings(defaults_object, result);

    auto overrides_object = object->get("overrides").extract<Poco::JSON::Object::Ptr>();
    parseCatalogConfigurationSettings(overrides_object, result);

    LOG_TEST(log, "Parsed catalog configuration settings: {}", result.toString());
    return result;
}

void RestCatalog::parseCatalogConfigurationSettings(const Poco::JSON::Object::Ptr & object, Config & result)
{
    if (!object)
        return;

    if (object->has("prefix"))
        result.prefix = object->get("prefix").extract<String>();

    if (object->has("default-base-location"))
        result.default_base_location = object->get("default-base-location").extract<String>();
}

DB::HTTPHeaderEntries RestCatalog::getAuthHeaders(
    const std::string & uri, const std::string & method, const std::shared_ptr<std::stringstream> & payload, bool update_token) const
{
    /// Option 1: user specified auth header manually.
    /// Header has format: 'Authorization: <scheme> <token>'.
    if (auth_header.has_value())
    {
        return DB::HTTPHeaderEntries{auth_header.value()};
    }

    /// Option 2: AWS Sign V4
    if (aws_auth_signer)
    {
        Aws::Http::URI url{uri};
        Aws::Http::HttpMethod aws_http_method;
        if (method == Poco::Net::HTTPRequest::HTTP_POST)
            aws_http_method = Aws::Http::HttpMethod::HTTP_POST;
        else if (method == Poco::Net::HTTPRequest::HTTP_DELETE)
            aws_http_method = Aws::Http::HttpMethod::HTTP_DELETE;
        else if (method == Poco::Net::HTTPRequest::HTTP_PUT)
            aws_http_method = Aws::Http::HttpMethod::HTTP_PUT;
        else if (method == Poco::Net::HTTPRequest::HTTP_HEAD)
            aws_http_method = Aws::Http::HttpMethod::HTTP_HEAD;
        else if (method == Poco::Net::HTTPRequest::HTTP_PATCH)
            aws_http_method = Aws::Http::HttpMethod::HTTP_PATCH;
        else
            aws_http_method = Aws::Http::HttpMethod::HTTP_GET;

        Aws::Http::Standard::StandardHttpRequest req{url, aws_http_method};
        req.AddContentBody(payload);
        if (!aws_auth_signer->SignRequest(req))
            throw DB::Exception(DB::ErrorCodes::AWS_ERROR, "Failed to sign REST catalog request");

        DB::HTTPHeaderEntries headers;
        for (const auto & header : Aws::Auth::AWSAuthHelper::CanonicalizeHeaders(req.GetHeaders()))
            headers.emplace_back(header.first, header.second);
        return headers;
    }

    /// Option 3: GCP OAuth
    if (rest_auth_type == "gcp_oauth")
    {
        if (!access_token.has_value() || update_token)
            access_token = retrieveGCPOAuthAccessToken();

        DB::HTTPHeaderEntries headers;
        headers.emplace_back("Authorization", "Bearer " + access_token.value());

        /// TODO: 'x-goog-user-project' is system parameter in using gcp no matter the auth type. Put it here as HTTP headers are set in variant functions.
        if (!gcp_project_id.empty())
            headers.emplace_back("x-goog-user-project", gcp_project_id);

        return headers;
    }

    /// Option 4: user provided grant_type, client_id and client_secret.
    /// We would make OAuthClientCredentialsRequest
    /// https://github.com/apache/iceberg/blob/3badfe0c1fcf0c0adfc7aa4a10f0b50365c48cf9/open-api/rest-catalog-open-api.yaml#L3498C5-L3498C34
    // if (!client_id.empty())
    // {
    //     if (!access_token.has_value() || update_token)
    //     {
    //         access_token = retrieveAccessToken();
    //     }

    //     DB::HTTPHeaderEntries headers;
    //     headers.emplace_back("Authorization", "Bearer " + access_token.value());
    //     return headers;
    // }

    return {};
}

std::string RestCatalog::retrieveGCPOAuthAccessToken() const
{
    Poco::URI url{oauth_server_uri};
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, url.toString(), Poco::Net::HTTPRequest::HTTP_1_1);
    request.add("metadata-flavor", "Google");
    const auto timeouts = DB::ConnectionTimeouts::getHTTPTimeouts(
        getContext()->getSettingsRef(), /*http_keep_alive_timeout_=*/Poco::Timespan(/*seconds=*/30, /*microseconds=*/0));
    auto session = makeHTTPSession(url, timeouts);
    session->sendRequest(request);

    String token_json_raw;
    Poco::Net::HTTPResponse response;
    try
    {
        auto & in = session->receiveResponse(response);
        Poco::StreamCopier::copyToString(in, token_json_raw);
    }
    catch (const std::exception & ex)
    {
        throw DB::Exception(DB::ErrorCodes::AUTHENTICATION_FAILED, "Failed to request GCP OAuth token: {}", ex.what());
    }

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        throw DB::Exception(DB::ErrorCodes::AUTHENTICATION_FAILED, "Failed to request GCP OAuth token: {}", response.getReason());

    Poco::JSON::Parser parser;
    auto object = parser.parse(token_json_raw).extract<Poco::JSON::Object::Ptr>();

    if (!object->has("access_token") || !object->has("expires_in") || !object->has("token_type"))
        throw DB::Exception(
            DB::ErrorCodes::AUTHENTICATION_FAILED,
            "Unexpected structure of response. Response should have fields: 'access_token', 'expires_in', 'token_type'");

    auto token_type = object->getValue<String>("token_type");
    if (token_type != "Bearer")
        throw DB::Exception(
            DB::ErrorCodes::AUTHENTICATION_FAILED, "Unexpected structure of response. Expected Bearer token, got {}", token_type);

    return object->getValue<String>("access_token");
}

std::string RestCatalog::retrieveAccessToken() const
{
#if 0
    static constexpr auto oauth_tokens_endpoint = "oauth/tokens";

    /// TODO:
    /// 1. support oauth2-server-uri
    /// https://github.com/apache/iceberg/blob/918f81f3c3f498f46afcea17c1ac9cdc6913cb5c/open-api/rest-catalog-open-api.yaml#L183C82-L183C99

    DB::HTTPHeaderEntries headers;
    headers.emplace_back("Content-Type", "application/x-www-form-urlencoded");
    headers.emplace_back("Accepts", "application/json; charset=UTF-8");

    Poco::URI url;
    DB::ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback;
    if (oauth_server_uri.empty())
    {
        url = Poco::URI(base_url / oauth_tokens_endpoint);

        Poco::URI::QueryParameters params = {
            {"grant_type", "client_credentials"},
            {"scope", auth_scope},
            {"client_id", client_id},
            {"client_secret", client_secret},
        };
        url.setQueryParameters(params);
    }
    else
    {
        url = Poco::URI(oauth_server_uri);
        out_stream_callback = [&](std::ostream & os)
        {
            os << fmt::format(
                "grant_type=client_credentials&scope={}&client_id={}&client_secret={}",
                auth_scope, client_id, client_secret);
        };
    }

    const auto & context = getContext();
    auto wb = DB::BuilderRWBufferFromHTTP(url)
        .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
        .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
        .withSettings(context->getReadSettings())
        .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
        .withHostFilter(&context->getRemoteHostFilter())
        .withOutCallback(std::move(out_stream_callback))
        .withSkipNotFound(false)
        .withHeaders(headers)
        .create(credentials);

    std::string json_str;
    readJSONObjectPossiblyInvalid(json_str, *wb);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var res_json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = res_json.extract<Poco::JSON::Object::Ptr>();

    return object->get("access_token").extract<String>();
#else
    return "";
#endif
}

std::optional<StorageType> RestCatalog::getStorageType() const
{
    if (config.default_base_location.empty())
        return std::nullopt;
    return parseStorageTypeFromLocation(config.default_base_location);
}

Poco::URI RestCatalog::buildURI(const String & endpoint) const
{
    /// Can't set enable_url_encoding to true here, or it will cause issues because the prefix of some catalog services (like S3 tables) contains encoded charactors, encoding it agian, will cause double-encoding.
    return Poco::URI{base_url / endpoint, /*enable_url_encoding=*/false};
}

std::unique_ptr<DB::ReadWriteBufferFromHTTP> RestCatalog::createReadBuffer(
    const std::string & endpoint,
    const std::string & method,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers) const
{
    const auto & context = getContext();

    auto url = buildURI(endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    auto create_buffer = [&](bool update_token) {
        auto result_headers = getAuthHeaders(url.toString(), method, nullptr, update_token);
        std::move(headers.begin(), headers.end(), std::back_inserter(result_headers));

        return std::make_unique<DB::ReadWriteBufferFromHTTP>(
            url,
            method,
            /*out_stream_callback_=*/nullptr,
            DB::ConnectionTimeouts::getHTTPTimeouts(
                context->getSettingsRef(), /*http_keep_alive_timeout_=*/Poco::Timespan(/*seconds=*/30, /*microseconds=*/0)),
            credentials,
            /*max_redirects=*/0,
            DBMS_DEFAULT_BUFFER_SIZE,
            getContext()->getReadSettings(),
            result_headers,
            &getContext()->getRemoteHostFilter(),
            /*delay_initialization_=*/false);
    };

    LOG_TEST(log, "Requesting: {}", url.toString());

    try
    {
        return create_buffer(false);
    }
    catch (const DB::HTTPException & e)
    {
        const auto status = e.getHTTPStatus();
        if (update_token_if_expired
            && (status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_UNAUTHORIZED
                || status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_FORBIDDEN))
        {
            return create_buffer(true);
        }
        throw;
    }
}

bool RestCatalog::empty(const std::string & namespace_name) const
{
    const auto tables = getTablesImpl(namespace_name, /*limit=*/1);
    return tables.empty();
}

bool RestCatalog::existsNamespace(const std::string & name) const
{
    try
    {
        /// It should use the HEAD /namespaces/<name> API for just checking if the namespace exist or not.
        /// However, not every catalog service implements this API (I am looking at you S3 table, as of March 16, 2025).
        createReadBuffer((config.prefix / NAMESPACES_ENDPOINT / encodeNamespaceName(name)).string(), Poco::Net::HTTPRequest::HTTP_GET);
    }
    catch (DB::HTTPException & e)
    {
        if (e.getHTTPStatus() == Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND)
            return false;

        e.addMessage(fmt::format("Failed to get namespace {}", name));
        e.rethrow();
    }

    return true;
}

void RestCatalog::createNamespace(const std::string & name [[maybe_unused]]) const
{
}

DB::Names RestCatalog::getTables(const std::string & namespace_name) const
{
    return getTablesImpl(namespace_name);
}

Poco::URI::QueryParameters RestCatalog::createParentNamespaceParams(const std::string & base_namespace) const
{
    return {{"parent", encodeNamespaceName(base_namespace)}};
}

DB::Names RestCatalog::getTablesImpl(const std::string & base_namespace, size_t limit) const
{
    const std::string endpoint = std::filesystem::path(NAMESPACES_ENDPOINT) / encodeNamespaceName(base_namespace) / "tables";
    auto buf = createReadBuffer(
        config.prefix / endpoint,
        Poco::Net::HTTPRequest::HTTP_GET,
        limit == 0 ? Poco::URI::QueryParameters{} : Poco::URI::QueryParameters{{"pageSize", fmt::format("{}", limit)}});
    /// A catalog might not support the pageSize parameter, so we still tell the parseTables function to only parse the limit number of tables.
    return parseTables(*buf, limit);
}

void RestCatalog::createTable(
    const std::string & namespace_name,
    const std::string & name,
    std::optional<const std::string> location,
    const DB::NamesAndTypesList & schema,
    std::optional<const std::string> partition_spec,
    std::optional<const std::string> write_order,
    bool stage_create,
    std::unordered_map<std::string, std::string> properties)
{
    Poco::JSON::Object payload;
    payload.set("name", name);
    payload.set("schema", generateIcebergSchema(schema));
    payload.set("stage-create", stage_create);
    if (location)
        payload.set("location", location.value());
    if (partition_spec) /// partition_spec should not be a string
        payload.set("partition-spec", "FIXME");
    if (write_order) /// write_order should not be a string
        payload.set("write-order", "FIXME");
    if (!properties.empty())
    {
        Poco::JSON::Object props;
        for (const auto & kv : properties)
            props.set(kv.first, kv.second);
        payload.set("properties", props);
    }

    auto oss = std::make_shared<std::stringstream>();
    payload.stringify(*oss);

    auto endpoint = buildURI((config.prefix / NAMESPACES_ENDPOINT / encodeNamespaceName(namespace_name) / "tables").string());

    auto auth_headers = getAuthHeaders(endpoint.toString(), Poco::Net::HTTPRequest::HTTP_POST, oss);
    std::vector<std::pair<String, String>> headers;
    headers.reserve(auth_headers.size());
    for (const auto & header : auth_headers)
        headers.emplace_back(header.name, header.value);

    auto resp = DB::sendRequest(
        endpoint,
        Poco::Net::HTTPRequest::HTTP_POST,
        /*query_id=*/"",
        /*user=*/"",
        /*password=*/"",
        oss->str(),
        headers,
        DB::ConnectionTimeouts().withConnectionTimeout(10).withSendTimeout(10).withReceiveTimeout(10),
        getLogger("IcebergRestCatalog::createTable"));

    if (resp.second >= 300)
        throw DB::Exception(
            DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Failed to create table, HTTP response code={}, body={}", resp.second, resp.first);
}

DB::Names RestCatalog::parseTables(DB::ReadBuffer & buf, size_t limit) const
{
    if (buf.eof())
        return {};

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, buf);

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

        auto identifiers_object = object->get("identifiers").extract<Poco::JSON::Array::Ptr>();
        if (!identifiers_object)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

        DB::Names tables;
        tables.reserve(identifiers_object->size());
        for (size_t i = 0; i < identifiers_object->size(); ++i)
        {
            const auto current_table_json = identifiers_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
            const auto table_name = current_table_json->get("name").extract<String>();

            tables.push_back(table_name);
            if (limit != 0 && tables.size() >= limit)
                break;
        }

        return tables;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

bool RestCatalog::existsTable(const std::string & namespace_name, const std::string & table_name) const
{
    TableMetadata table_metadata;
    return tryGetTableMetadata(namespace_name, table_name, table_metadata);
}

void RestCatalog::deleteTable(const std::string & namespace_name, const std::string & table_name)
{
    Poco::URI endpoint
        = buildURI((config.prefix / NAMESPACES_ENDPOINT / encodeNamespaceName(namespace_name) / "tables" / table_name).string());

    auto auth_headers = getAuthHeaders(endpoint.toString(), Poco::Net::HTTPRequest::HTTP_DELETE);
    std::vector<std::pair<String, String>> headers;
    headers.reserve(auth_headers.size());
    for (const auto & header : auth_headers)
        headers.emplace_back(header.name, header.value);

    auto resp = DB::sendRequest(
        endpoint,
        Poco::Net::HTTPRequest::HTTP_DELETE,
        /*query_id=*/"",
        /*user=*/"",
        /*password=*/"",
        /*payload=*/"",
        headers,
        DB::ConnectionTimeouts().withConnectionTimeout(10).withSendTimeout(10).withReceiveTimeout(10),
        getLogger("IcebergRestCatalog::deleteTable"));

    if (resp.second >= 300)
        throw DB::Exception(
            DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Failed to delete table, HTTP response code={}, body={}", resp.second, resp.first);
}

namespace
{
void extractTableMetadata(const std::string & json_str, TableMetadata & result, LoggerPtr log)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

    auto metadata_object = object->get("metadata").extract<Poco::JSON::Object::Ptr>();
    if (!metadata_object)
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Cannot parse table metadata json string");

    if (!metadata_object->has("format-version"))
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Table metadata missing required field: format-version");
    int format_version = metadata_object->getValue<int>("format-version");
    result.setFormatVersion(format_version);

    /// last-sequence-number is v2+ only; v1 tables omit it
    result.setSequenceNumber(metadata_object->optValue<uint64_t>("last-sequence-number", 0));

    if (!metadata_object->has("table-uuid"))
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Table metadata missing required field: table-uuid");
    result.setTableUUID(metadata_object->getValue<std::string>("table-uuid"));

    int64_t snapshot_id = 0;
    if (metadata_object->has("current-snapshot-id"))
    {
        snapshot_id = metadata_object->getValue<int64_t>("current-snapshot-id");
        result.setSnapshotID(snapshot_id);
    }

    auto snapshots = metadata_object->get("snapshots");
    if (!snapshots.isEmpty() && snapshots.isArray())
    {
        const auto & snapshots_array = snapshots.extract<Poco::JSON::Array::Ptr>();
        for (size_t i = 0; i < snapshots_array->size(); i++)
        {
            auto snapshot_obj = snapshots_array->get(static_cast<uint32_t>(i)).extract<Poco::JSON::Object::Ptr>();
            if (!snapshot_obj->has("snapshot-id"))
                throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Snapshot missing required field: snapshot-id");
            if (snapshot_obj->getValue<int64_t>("snapshot-id") == snapshot_id)
            {
                Snapshot snapshot;
                snapshot.snapshot_id = snapshot_id;
                /// sequence-number is v2+ only; v1 snapshots omit it
                snapshot.sequence_number = snapshot_obj->optValue<uint64_t>("sequence-number", 0);
                if (!snapshot_obj->has("timestamp-ms"))
                    throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Snapshot missing required field: timestamp-ms");
                snapshot.timestamp_ms = snapshot_obj->getValue<int64_t>("timestamp-ms");
                if (!snapshot_obj->has("manifest-list"))
                    throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Snapshot missing required field: manifest-list");
                snapshot.manifest_list = snapshot_obj->getValue<std::string>("manifest-list");
                snapshot.schema_id = snapshot_obj->optValue<int64_t>("schema-id", 0);

                auto summary_obj = snapshot_obj->get("summary").extract<Poco::JSON::Object::Ptr>();
                if (!summary_obj)
                    throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Snapshot missing required field: summary");
                if (!summary_obj->has("operation"))
                    throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Snapshot summary missing required field: operation");
                snapshot.summary.operation = summary_obj->getValue<std::string>("operation");
                /// summary optional fields
                if (summary_obj->has("added-files-size"))
                    snapshot.summary.added_files_size = summary_obj->getValue<std::string>("added-files-size");
                if (summary_obj->has("added-data-files"))
                    snapshot.summary.added_data_files = summary_obj->getValue<std::string>("added-data-files");
                if (summary_obj->has("added-records"))
                    snapshot.summary.added_records = summary_obj->getValue<std::string>("added-records");
                if (summary_obj->has("total-data-files"))
                    snapshot.summary.total_data_files = summary_obj->getValue<std::string>("total-data-files");
                if (summary_obj->has("total-delete-files"))
                    snapshot.summary.total_delete_files = summary_obj->getValue<std::string>("total-delete-files");
                if (summary_obj->has("total-records"))
                    snapshot.summary.total_records = summary_obj->getValue<std::string>("total-records");
                if (summary_obj->has("total-files-size"))
                    snapshot.summary.total_files_size = summary_obj->getValue<std::string>("total-files-size");
                if (summary_obj->has("total-position-deletes"))
                    snapshot.summary.total_position_deletes = summary_obj->getValue<std::string>("total-position-deletes");
                if (summary_obj->has("total-equality-deletes"))
                    snapshot.summary.total_equality_deletes = summary_obj->getValue<std::string>("total-equality-deletes");

                result.setCurrentSnapshot(std::move(snapshot));
                break;
            }
        }
    }
    else
    {
        LOG_INFO(log, "Table metadata has no snapshots.");
    }

    std::string location;
    if (result.requiresLocation())
    {
        location = metadata_object->get("location").extract<String>();
        result.setLocation(location);
    }

    if (result.requiresSchema())
    {
        auto schema_processor = DB::IcebergSchemaProcessor();
        std::string iceberg_schema_json;
        auto id = DB::IcebergMetadata::parseTableSchema(metadata_object, schema_processor, iceberg_schema_json, log);
        auto schema = schema_processor.getTimeplusTableSchemaById(id);
        result.setSchema(*schema);
        result.setSchemaJSON(iceberg_schema_json);
        result.setCurrentSchemaID(id);
    }

    if (result.requiresCredentials() && object->has("config"))
    {
        auto config_object = object->get("config").extract<Poco::JSON::Object::Ptr>();
        if (!config_object)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse config result");

        auto storage_type = parseStorageTypeFromLocation(location);
        switch (storage_type)
        {
            case StorageType::S3Tables:
                [[fallthrough]];
            case StorageType::S3:
            {
                static constexpr auto access_key_id_str = "s3.access-key-id";
                static constexpr auto secret_access_key_str = "s3.secret-access-key";
                static constexpr auto session_token_str = "s3.session-token";

                std::string access_key_id;
                std::string secret_access_key;
                std::string session_token;
                if (config_object->has(access_key_id_str))
                    access_key_id = config_object->get(access_key_id_str).extract<String>();
                if (config_object->has(secret_access_key_str))
                    secret_access_key = config_object->get(secret_access_key_str).extract<String>();
                if (config_object->has(session_token_str))
                    session_token = config_object->get(session_token_str).extract<String>();

                result.setStorageCredentials(std::make_shared<S3Credentials>(access_key_id, secret_access_key, session_token));
                break;
            }
            default:
                break;
        }
    }
}
}

void RestCatalog::commitTable(
    const std::string & namespace_name,
    const std::string & name,
    const Requirements & requirements,
    const Updates & updates,
    TableMetadata & table_metadata)
{
    Poco::JSON::Object payload;

    Poco::JSON::Array namespaces;
    {
        std::vector<std::string> parts;
        splitInto<'.'>(parts, namespace_name);
        for (const auto & part : parts)
            namespaces.add(part);
    }

    Poco::JSON::Object identifier;
    identifier.set("namespace", namespaces);
    identifier.set("name", name);
    payload.set("identifier", identifier);

    Poco::JSON::Array the_requirements;
    for (const auto & requirement : requirements)
        requirement->apply(the_requirements);
    payload.set("requirements", the_requirements);

    Poco::JSON::Array the_updates;
    for (const auto & update : updates)
        update->apply(the_updates);
    payload.set("updates", the_updates);

    auto payload_json = std::make_shared<std::stringstream>();
    payload.stringify(*payload_json);

    Poco::URI endpoint = buildURI((config.prefix / NAMESPACES_ENDPOINT / encodeNamespaceName(namespace_name) / "tables" / name).string());

    auto auth_headers = getAuthHeaders(endpoint.toString(), Poco::Net::HTTPRequest::HTTP_POST, payload_json);
    std::vector<std::pair<String, String>> headers;
    headers.reserve(auth_headers.size());
    for (const auto & header : auth_headers)
        headers.emplace_back(header.name, header.value);

    auto request_body = payload_json->str();

    auto resp = DB::sendRequest(
        Poco::URI(endpoint),
        Poco::Net::HTTPRequest::HTTP_POST,
        /*query_id=*/"",
        /*user=*/"",
        /*password=*/"",
        request_body,
        headers,
        DB::ConnectionTimeouts().withConnectionTimeout(10).withSendTimeout(10).withReceiveTimeout(10),
        getLogger("IcebergRestCatalog::commitTable"));

    if (resp.second >= 300)
        throw DB::Exception(
            DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Failed to commit table, HTTP response code={}, body={}", resp.second, resp.first);

    extractTableMetadata(resp.first, table_metadata, log);
}

bool RestCatalog::tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    try
    {
        return getTableMetadataImpl(namespace_name, table_name, result);
    }
    catch (...)
    {
        DB::tryLogCurrentException(log->name().data());
        return false;
    }
}

void RestCatalog::getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    if (!getTableMetadataImpl(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "Table not found from iceberg catalog: namespace={} table={}", namespace_name, table_name);
}

bool RestCatalog::getTableMetadataImpl(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const
{
    LOG_TEST(log, "Checking table {} in namespace {}", table_name, namespace_name);

    DB::HTTPHeaderEntries headers;
    if (result.requiresCredentials())
    {
        /// Header `X-Iceberg-Access-Delegation` tells catalog to include storage credentials in LoadTableResponse.
        /// Value can be one of the two:
        /// 1. `vended-credentials`
        /// 2. `remote-signing`
        /// Currently we support only the first.
        /// https://github.com/apache/iceberg/blob/3badfe0c1fcf0c0adfc7aa4a10f0b50365c48cf9/open-api/rest-catalog-open-api.yaml#L1832
        headers.emplace_back("X-Iceberg-Access-Delegation", "vended-credentials");
    }

    const std::string endpoint = std::filesystem::path(NAMESPACES_ENDPOINT) / encodeNamespaceName(namespace_name) / "tables" / table_name;
    try
    {
        auto buf = createReadBuffer(config.prefix / endpoint, Poco::Net::HTTPRequest::HTTP_GET, /* params */ {}, headers);

        if (buf->eof())
        {
            LOG_TEST(log, "Table doesn't exist (endpoint: {})", endpoint);
            return false;
        }

        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);

        extractTableMetadata(json_str, result, log);

        return true;
    }
    catch (DB::HTTPException & e)
    {
        if (e.getHTTPStatus() == 404)
            return false;

        e.addMessage("Failed to fetch table metadata of table {} in namespace {}", table_name, namespace_name);
        e.rethrow();
    }

    return false;
}

/*
{
  "identifier": {
    "namespace": [
      "gimitest"
    ],
    "name": "city_locations"
  },
  "requirements": [
    {
      "type": "assert-ref-snapshot-id",
      "ref": "main"
    },
    {
      "type": "assert-table-uuid",
      "uuid": "d13a7cd8-8518-4003-aa1a-4dfc4308119e"
    }
  ],
  "updates": [
    {
      "action": "add-snapshot",
      "snapshot": {
        "snapshot-id": 4488436330293905232,
        ["parent-snapshot-id": xxx,]
        "sequence-number": 1,
        "timestamp-ms": 1738547253392,
        "manifest-list": "s3://tp-gimi-test/city_locations/metadata/snap-4488436330293905232-0-e0c4d753-e51b-476a-9415-a61bcd479b83.avro",
        "summary": {
          "operation": "append",
          "added-files-size": "1373",
          "added-data-files": "1",
          "added-records": "4",
          "total-data-files": "1",
          "total-delete-files": "0",
          "total-records": "4",
          "total-files-size": "1373",
          "total-position-deletes": "0",
          "total-equality-deletes": "0"
        },
        "schema-id": 0
      }
    },
    {
      "action": "set-snapshot-ref",
      "ref-name": "main",
      "type": "branch",
      "snapshot-id": 4488436330293905232
    }
  ]
}
*/

}

#endif
