#pragma once

#include "config.h"

#if USE_AVRO
#include <filesystem>
#include <Storages/Iceberg/ICatalog.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/JSON/Object.h>
#include <Poco/Net/HTTPBasicCredentials.h>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>

namespace DB
{
class ReadBuffer;
}

namespace Apache::Iceberg
{

class RestCatalog final : public ICatalog, private DB::WithContext
{
public:
    struct Options
    {
        std::string catalog_credential;
        std::string auth_scope;
        std::string auth_header;
        std::string oauth_server_uri;

        bool use_environment_credentials;
        bool enable_sigv4{false};
        std::string signing_region;
        std::string signing_name;
    };

private:
    struct Config
    {
        /// Prefix is a path of the catalog endpoint,
        /// e.g. /v1/{prefix}/namespaces/{namespace}/tables/{table}
        std::filesystem::path prefix;
        /// Base location is location of data in storage
        /// (in filesystem or object storage).
        std::string default_base_location;

        std::string toString() const;
    };

    using StopCondition = std::function<bool(const std::string & namespace_name)>;
    using ExecuteFunc = std::function<void(const std::string & namespace_name)>;

public:
    explicit RestCatalog(const std::string & warehouse_, const std::string & base_url_, const Options & options, DB::ContextPtr context_);

    ~RestCatalog() override = default;

    bool existsNamespace(const std::string & name) const override;

    void createNamespace(const std::string & name) const override;

    bool empty(const std::string & namespace_name) const override;

    DB::Names getTables(const std::string & namespace_name) const override;

    bool existsTable(const std::string & namespace_name, const std::string & table_name) const override;

    void createTable(
        const std::string & namespace_name,
        const std::string & name,
        std::optional<const std::string> location,
        const DB::NamesAndTypesList & schema,
        std::optional<const std::string> partition_spec,
        std::optional<const std::string> write_order,
        bool stage_create,
        std::unordered_map<std::string, std::string> properties) override;

    void deleteTable(const std::string & namespace_name, const std::string & name) override;

    void commitTable(
        const std::string & namespace_name,
        const std::string & name,
        const Requirements & requirements,
        const Updates & updates,
        TableMetadata & table_metadata) override;

    void getTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    bool tryGetTableMetadata(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const override;

    std::optional<StorageType> getStorageType() const override;

private:
    static void parseCatalogConfigurationSettings(const Poco::JSON::Object::Ptr & object, Config & result);

    Poco::URI buildURI(const String & endpoint) const;

    std::unique_ptr<DB::ReadWriteBufferFromHTTP> createReadBuffer(
        const std::string & endpoint,
        const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
        const Poco::URI::QueryParameters & params = {},
        const DB::HTTPHeaderEntries & headers = {}) const;

    Poco::URI::QueryParameters createParentNamespaceParams(const std::string & base_namespace) const;

    void getNamespacesRecursive_deleted(
        const std::string & base_namespace, Namespaces & result, StopCondition stop_condition, ExecuteFunc func) const;

    DB::Names getTablesImpl(const std::string & base_namespace, size_t limit = 0) const;

    DB::Names parseTables(DB::ReadBuffer & buf, size_t limit) const;

    bool getTableMetadataImpl(const std::string & namespace_name, const std::string & table_name, TableMetadata & result) const;

    DB::HTTPHeaderEntries getAuthHeaders(
        const std::string & uri,
        const std::string & method = Poco::Net::HTTPRequest::HTTP_GET,
        const std::shared_ptr<std::stringstream> & payload = nullptr,
        bool update_token = false) const;

    std::string retrieveAccessToken() const;

    Config loadConfig();

    std::filesystem::path base_url;
    LoggerPtr log;

    /// Catalog configuration settings from /v1/config endpoint.
    Config config;

    /// Auth headers of format: "Authorization": "<auth_scheme> <token>"
    std::optional<DB::HTTPHeaderEntry> auth_header;

    /// Parameters for OAuth.
    bool update_token_if_expired = false;
    std::string client_id;
    std::string client_secret;
    std::string auth_scope;
    std::string oauth_server_uri;
    mutable std::optional<std::string> access_token;

    Poco::Net::HTTPBasicCredentials credentials;

    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> aws_credentials;
    std::shared_ptr<const Aws::Client::AWSAuthV4Signer> aws_auth_signer;

    ThreadPool pool;
};

}

#endif
