#pragma once

#include "config_version.h"

#include <Core/ServerUUID.h>

#include <Poco/JSON/Object.h>

#include <string>

namespace DB
{

namespace
{
const std::string DDL_CREATE = "CREATE";
}

class TelemetryElement
{
public:
    TelemetryElement() : object(new Poco::JSON::Object) { }

    ~TelemetryElement() = default;

    std::string toString() const
    {
        std::stringstream oss;
        object->stringify(oss);
        return oss.str();
    }

    Poco::JSON::Object::Ptr getObject() const { return object; }

private:
    Poco::JSON::Object::Ptr object;
};


class TelemetryElementBuilderBase
{
public:
    using Self = TelemetryElementBuilderBase;

    TelemetryElementBuilderBase(const std::string & event_type) : element(std::make_shared<TelemetryElement>())
    {
        getObject()->set("type", "track");
        getObject()->set("event", event_type);

        properties = new Poco::JSON::Object;
        getObject()->set("properties", properties);

        withServerUUID();
    }

    std::shared_ptr<TelemetryElement> build() { return std::move(element); }

protected:
    std::shared_ptr<TelemetryElement> getElement() const { return element; }

    Poco::JSON::Object::Ptr getObject() const { return element->getObject(); }

    template <typename T>
    void setProperty(const std::string & name, T && value)
    {
        properties->set(name, std::forward<T>(value));
    }

    void withServerUUID()
    {
        DB::UUID server_uuid = DB::ServerUUID::get();
        std::string server_uuid_str = server_uuid != DB::UUIDHelpers::Nil ? DB::toString(server_uuid) : "Unknown";
        setProperty("server_id", server_uuid_str);
    }

private:
    std::shared_ptr<TelemetryElement> element;
    Poco::JSON::Object::Ptr properties;
};


class TelemetryStatsElementBuilder final : public TelemetryElementBuilderBase
{
public:
    using Self = TelemetryStatsElementBuilder;

    TelemetryStatsElementBuilder() : TelemetryElementBuilderBase("proton_ping")
    {
        setEdition();
        setVersion();
    }

    Self & useCPU(unsigned int cpu_)
    {
        setProperty("cpu", cpu_);
        return *this;
    }

    Self & useMemoryInGB(uint64_t memory_in_gb_)
    {
        setProperty("memory_in_gb", memory_in_gb_);
        return *this;
    }

    Self & isInDocker(bool in_docker_)
    {
        setProperty("docker", in_docker_);
        return *this;
    }

    Self & isNewSession(bool new_session_)
    {
        setProperty("new_session", new_session_);
        return *this;
    }

    Self & startedOn(const std::string & started_on_)
    {
        setProperty("started_on", started_on_);
        return *this;
    }

    Self & during(int64_t duration_in_minute_)
    {
        setProperty("duration_in_minute", duration_in_minute_);
        return *this;
    }

    Self & setTotalSelectQuery(int64_t total_select_query_)
    {
        setProperty("total_select_query", total_select_query_);
        return *this;
    }

    Self & setHistoricalSelectQuery(int64_t historical_select_query_)
    {
        setProperty("historical_select_query", historical_select_query_);
        return *this;
    }

    Self & setStreamingSelectQuery(int64_t streaming_select_query_)
    {
        setProperty("streaming_select_query", streaming_select_query_);
        return *this;
    }

    Self & setDeltaTotalSelectQuery(int64_t delta_total_select_query_)
    {
        setProperty("delta_total_select_query", delta_total_select_query_);
        return *this;
    }

    Self & setDeltaHistoricalSelectQuery(int64_t delta_historical_select_query_)
    {
        setProperty("delta_historical_select_query", delta_historical_select_query_);
        return *this;
    }

    Self & setDeltaStreamingSelectQuery(int64_t delta_streaming_select_query_)
    {
        setProperty("delta_streaming_select_query", delta_streaming_select_query_);
        return *this;
    }

private:
    Self & setEdition()
    {
        setProperty("edition", EDITION);
        return *this;
    }

    Self & setVersion()
    {
        setProperty("version", VERSION_STRING);
        return *this;
    }
};

class DDLTelemetryElementBuilderBase : public TelemetryElementBuilderBase
{
public:
    using Self = DDLTelemetryElementBuilderBase;

    DDLTelemetryElementBuilderBase(const std::string & ddl_type, const std::string & resource_type)
        : TelemetryElementBuilderBase("proton_ddl")
    {
        withDDLType(ddl_type);
        withResourceType(resource_type);
    }

    DDLTelemetryElementBuilderBase & withDDLType(const std::string & ddl_type)
    {
        setProperty("ddl_type", ddl_type);
        return *this;
    }

    DDLTelemetryElementBuilderBase & withResourceType(const std::string & resource_type)
    {
        setProperty("resource_type", resource_type);
        return *this;
    }

    DDLTelemetryElementBuilderBase & withSubResourceType(const std::string & subresource_type)
    {
        setProperty("subresource_type", subresource_type);
        return *this;
    }
};

template <typename T>
class CreateStreamTelemetryElementBuilderBase : public DDLTelemetryElementBuilderBase
{
public:
    CreateStreamTelemetryElementBuilderBase(const std::string & subresource_type) : DDLTelemetryElementBuilderBase(DDL_CREATE, "stream")
    {
        withSubResourceType(subresource_type);
    }
};

class CreateStreamTelemetryElementBuilder : public CreateStreamTelemetryElementBuilderBase<CreateStreamTelemetryElementBuilder>
{
public:
    using Base = CreateStreamTelemetryElementBuilderBase<CreateStreamTelemetryElementBuilder>;
    using Self = CreateStreamTelemetryElementBuilder;

    CreateStreamTelemetryElementBuilder() : Base("stream") { }

    Self & withShards(uint32_t shards)
    {
        setProperty("shards", shards);
        return *this;
    }

    Self & withRepliactionFactor(uint32_t replication_factor)
    {
        setProperty("replication_factor", replication_factor);
        return *this;
    }

    Self & withStorageType(const std::string & storage_type)
    {
        setProperty("storage_type", storage_type);
        return *this;
    }

    Self & withMode(const std::string & mode)
    {
        setProperty("mode", mode);
        return *this;
    }
};

class CreateExternalStreamTelemetryElementBuilder
    : public CreateStreamTelemetryElementBuilderBase<CreateExternalStreamTelemetryElementBuilder>
{
public:
    using Base = CreateStreamTelemetryElementBuilderBase<CreateExternalStreamTelemetryElementBuilder>;
    using Self = CreateExternalStreamTelemetryElementBuilder;

    CreateExternalStreamTelemetryElementBuilder() : Base("external_stream") { }

    Self & withType(const std::string & external_stream_type)
    {
        setProperty("external_stream_type", external_stream_type);
        return *this;
    }

    Self & withSecurityProtocol(const std::string & security_protocol)
    {
        setProperty("security_protocol", security_protocol);
        return *this;
    }

    Self & withSaslMechanism(const std::string & sasl_mechanism)
    {
        setProperty("sasl_mechanism", sasl_mechanism);
        return *this;
    }

    Self & withDataFormat(const std::string & data_format)
    {
        setProperty("data_format", data_format);
        return *this;
    }

    Self & withKafkaSchemaRegistryUrl(bool kafka_schema_registry)
    {
        setProperty("kafka_schema_registry", kafka_schema_registry);
        return *this;
    }

    Self & skipSslCertCheck(bool skip_ssl_cert_check)
    {
        setProperty("skip_ssl_cert_check", skip_ssl_cert_check);
        return *this;
    }

    Self & hasSslCaCertFile(bool has_ssl_ca_cert_file)
    {
        setProperty("has_ssl_ca_cert_file", has_ssl_ca_cert_file);
        return *this;
    }

    Self & hasSslCaPem(bool has_ssl_ca_pem)
    {
        setProperty("has_ssl_ca_pem", has_ssl_ca_pem);
        return *this;
    }

    Self & isSecure(bool is_secure)
    {
        setProperty("secure", is_secure);
        return *this;
    }

    Self & withFormatSchema(const std::string & format_schema)
    {
        setProperty("format_schema", format_schema);
        return *this;
    }

    Self & isOneMessagePerRow(bool is_one_message_per_row)
    {
        setProperty("one_message_per_row", is_one_message_per_row);
        return *this;
    }

    Self & skipServerCertCheck(bool skip_server_cert_check)
    {
        setProperty("skip_server_cert_check", skip_server_cert_check);
        return *this;
    }

    Self & validateHostname(bool validate_hostname)
    {
        setProperty("validate_hostname", validate_hostname);
        return *this;
    }
};

class CreateExternalTableTelemetryElementBuilder
    : public CreateStreamTelemetryElementBuilderBase<CreateExternalTableTelemetryElementBuilder>
{
public:
    using Base = CreateStreamTelemetryElementBuilderBase<CreateExternalTableTelemetryElementBuilder>;
    using Self = CreateExternalTableTelemetryElementBuilder;

    CreateExternalTableTelemetryElementBuilder() : Base("external_table") { }

    CreateExternalTableTelemetryElementBuilder & withType(const std::string & external_table_type)
    {
        setProperty("external_table_type", external_table_type);
        return *this;
    }

    Self & withSecure(const std::string & secure)
    {
        setProperty("secure", secure);
        return *this;
    }
};

class CreateMaterializedViewTelemetryElementBuilder
    : public CreateStreamTelemetryElementBuilderBase<CreateMaterializedViewTelemetryElementBuilder>
{
public:
    using Base = CreateStreamTelemetryElementBuilderBase<CreateMaterializedViewTelemetryElementBuilder>;
    using Self = CreateMaterializedViewTelemetryElementBuilder;

    CreateMaterializedViewTelemetryElementBuilder() : Base("materialized_view") { }

    Self & isExternalTarget(bool is_external_target)
    {
        setProperty("is_external_target", is_external_target);
        return *this;
    }

    Self & withShards(uint32_t shards)
    {
        setProperty("shards", shards);
        return *this;
    }

    Self & withRepliactionFactor(uint32_t replication_factor)
    {
        setProperty("replication_factor", replication_factor);
        return *this;
    }

    Self & withTargetStorageType(const std::string & storage_type)
    {
        setProperty("target_storage_type", storage_type);
        return *this;
    }
};

class CreateRandomStreamTelelemtryElementBuilder
    : public CreateStreamTelemetryElementBuilderBase<CreateRandomStreamTelelemtryElementBuilder>
{
public:
    using Base = CreateStreamTelemetryElementBuilderBase<CreateRandomStreamTelelemtryElementBuilder>;
    using Self = CreateRandomStreamTelelemtryElementBuilder;

    CreateRandomStreamTelelemtryElementBuilder() : Base("random_stream") { }

    Self & withEventsPerSecond(double events_per_second)
    {
        setProperty("eps", events_per_second);
        return *this;
    }
};
}
