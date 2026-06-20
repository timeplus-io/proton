#pragma once
#include "config.h"

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/SerdeTag.h>
#include <Cluster/Protocol/UDFType.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
SERDE struct UserDefinedFunctionPayload
{
public:
    UserDefinedFunctionPayload(const UserDefinedFunctionPayload &) = default;
    UserDefinedFunctionPayload() = default;

    virtual void serialize(DB::WriteBuffer & wb, uint16_t version_) const = 0;
    virtual void deserialize(DB::ReadBuffer & rb, uint16_t version_) = 0;
    virtual size_t approximateSerializedSize() const noexcept = 0;
    virtual bool operator==(const UserDefinedFunctionPayload & rhs) const = 0;
    virtual std::string string() const = 0;
    virtual UDFType type() const noexcept = 0;
    virtual bool isAggregation() const noexcept = 0;
    virtual std::string_view sourceView() const noexcept = 0;
    virtual std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const
        = 0;

    virtual ~UserDefinedFunctionPayload() = default;
    UserDefinedFunctionPayload & operator=(const UserDefinedFunctionPayload &) = default;
};
using UserDefinedFunctionPayloadPtr = std::shared_ptr<UserDefinedFunctionPayload>;


SERDE class ExecutableUserDefinedFunctionPayload final : public UserDefinedFunctionPayload
{
public:
    ExecutableUserDefinedFunctionPayload() = default;
    ExecutableUserDefinedFunctionPayload(const ExecutableUserDefinedFunctionPayload &) = default;
    ExecutableUserDefinedFunctionPayload(
        const std::string & command_,
        const std::vector<std::string> & command_arguments_,
        const std::string & format_,
        uint64_t command_termination_timeout_seconds_,
        uint64_t command_read_timeout_milliseconds_,
        uint64_t command_write_timeout_milliseconds_,
        uint64_t max_command_execution_time_seconds_,
        uint64_t pool_size_,
        bool is_executable_pool_,
        bool send_chunk_header_,
        bool execute_direct_,
        uint32_t data_version_)
        : data_version(data_version_)
        , command(command_)
        , command_arguments(command_arguments_)
        , format(format_)
        , command_termination_timeout_seconds(command_termination_timeout_seconds_)
        , command_read_timeout_milliseconds(command_read_timeout_milliseconds_)
        , command_write_timeout_milliseconds(command_write_timeout_milliseconds_)
        , max_command_execution_time_seconds(max_command_execution_time_seconds_)
        , pool_size(pool_size_)
        , is_executable_pool(is_executable_pool_)
        , send_chunk_header(send_chunk_header_)
        , execute_direct(execute_direct_)
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const override;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_) override;
    size_t approximateSerializedSize() const noexcept override;
    bool operator==(const UserDefinedFunctionPayload & rhs) const override;
    std::string string() const override;
    UDFType type() const noexcept override { return UDFType::Executable; }
    bool isAggregation() const noexcept override { return false; }
    std::string_view sourceView() const noexcept override { return ""; }
    std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const override;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    /// executable file name, only available when 'type' is 'executable'
    std::string command;
    std::vector<std::string> command_arguments;

    /// Script output format
    std::string format;

    /// Command termination timeout in seconds
    uint64_t command_termination_timeout_seconds = 10;

    /// Timeout for reading data from command stdout
    uint64_t command_read_timeout_milliseconds = 10000;

    /// Timeout for writing data to command stdin
    uint64_t command_write_timeout_milliseconds = 10000;

    /// Max command execution time in milliseconds. Valid only if executable_pool = true
    uint64_t max_command_execution_time_seconds = 10;

    /// Pool size valid only if executable_pool = true
    uint64_t pool_size = 16;

    /// Should pool of processes be created.
    bool is_executable_pool = false;

    /// Send number_of_rows\n before sending chunk to process.
    bool send_chunk_header = false;

    /// Execute script direct or with /bin/bash.
    bool execute_direct = true;
};


SERDE class RemoteUserDefinedFunctionPayload final : public UserDefinedFunctionPayload
{
public:
    enum AuthMethod : uint8_t
    {
        None = 0,
        AuthHeader = 1
    };

    RemoteUserDefinedFunctionPayload() = default;
    RemoteUserDefinedFunctionPayload(const RemoteUserDefinedFunctionPayload &) = default;
    RemoteUserDefinedFunctionPayload(
        uint64_t command_read_timeout_milliseconds_,
        uint64_t command_execution_timeout_milliseconds_,
        const std::string & url_,
        RemoteUserDefinedFunctionPayload::AuthMethod auth_method_,
        const std::string & auth_key_name_,
        const std::string & auth_key_value_,
        uint32_t data_version_)
        : data_version(data_version_)
        , command_read_timeout_milliseconds(command_read_timeout_milliseconds_)
        , command_execution_timeout_milliseconds(command_execution_timeout_milliseconds_)
        , url(url_)
        , auth_method(auth_method_)
        , auth_context({auth_key_name_, auth_key_value_})
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const override;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_) override;
    size_t approximateSerializedSize() const noexcept override;
    bool operator==(const UserDefinedFunctionPayload & rhs) const override;
    std::string string() const override;
    UDFType type() const noexcept override { return UDFType::Remote; }
    bool isAggregation() const noexcept override { return false; }
    std::string_view sourceView() const noexcept override { return ""; }
    std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const override;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 2;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    /// Timeout for reading data from input format
    uint64_t command_read_timeout_milliseconds = 10000;

    // Timeout for receiving response from remote endpoint
    uint64_t command_execution_timeout_milliseconds = 10000;

    /// url of remote endpoint, only available when 'type' is 'remote'
    std::string url;

    AuthMethod auth_method = AuthMethod::None;
    struct AuthContext
    {
        /// authorization header name, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_name;
        /// authorization header value, only available when 'type' is 'remote' and 'auth_method' is AUTH_HEADER
        std::string key_value;

        bool operator==(const AuthContext & rhs) const { return key_name == rhs.key_name && key_value == rhs.key_value; }
    };

    AuthContext auth_context;
};


SERDE class JavaScriptUserDefinedFunctionPayload final : public UserDefinedFunctionPayload
{
public:
    JavaScriptUserDefinedFunctionPayload() = default;
    JavaScriptUserDefinedFunctionPayload(const JavaScriptUserDefinedFunctionPayload &) = default;
    JavaScriptUserDefinedFunctionPayload(bool is_aggregation_, const std::string & source_, uint32_t data_version_)
        : data_version(data_version_), is_aggregation(is_aggregation_), source(source_)
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const override;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_) override;
    size_t approximateSerializedSize() const noexcept override;
    bool operator==(const UserDefinedFunctionPayload & rhs) const override;
    std::string string() const override;
    UDFType type() const noexcept override { return UDFType::Javascript; }
    bool isAggregation() const noexcept override { return is_aggregation; }
    std::string_view sourceView() const noexcept override { return source; }
    std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const override;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    /// whether the UDF is a aggregation function
    bool is_aggregation = false;

    /// source code of function, only available when 'type' is 'javascript'
    std::string source;
};
using JavaScriptUserDefinedFunctionPayloadPtr = std::shared_ptr<JavaScriptUserDefinedFunctionPayload>;

SERDE class PythonUserDefinedFunctionPayload final : public UserDefinedFunctionPayload
{
public:
    PythonUserDefinedFunctionPayload() = default;
    PythonUserDefinedFunctionPayload(const PythonUserDefinedFunctionPayload &) = default;
    PythonUserDefinedFunctionPayload & operator=(const PythonUserDefinedFunctionPayload &) = default;
    PythonUserDefinedFunctionPayload(bool is_aggregation_, const std::string & source_, uint32_t data_version_)
        : data_version(data_version_), is_aggregation(is_aggregation_), source(source_)
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const override;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_) override;
    size_t approximateSerializedSize() const noexcept override;
    bool operator==(const UserDefinedFunctionPayload & rhs) const override;
    std::string string() const override;
    UDFType type() const noexcept override { return UDFType::Python; }
    bool isAggregation() const noexcept override { return is_aggregation; }
    std::string_view sourceView() const noexcept override { return source; }
    std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const override;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    /// v2: added init_function_name / init_function_parameters / named_collection.
    constexpr static uint32_t schema_version = 2;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    bool is_aggregation = false;

    bool using_numpy = false;

    std::string source;

    /// Added in schema_version 2: optional init hook configuration.
    /// `named_collection` stores only the collection NAME; its values are
    /// resolved at UDF load time so secrets never persist in UDF metadata.
    std::string init_function_name;
    std::string init_function_parameters;
    std::string named_collection;
};
using PythonUserDefinedFunctionPayloadPtr = std::shared_ptr<PythonUserDefinedFunctionPayload>;

SERDE class SQLUserDefinedFunctionPayload final : public UserDefinedFunctionPayload
{
public:
    SQLUserDefinedFunctionPayload() = default;
    SQLUserDefinedFunctionPayload(const SQLUserDefinedFunctionPayload &) = default;
    SQLUserDefinedFunctionPayload(const std::string & query_, uint32_t data_version_) : data_version(data_version_), query(query_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const override;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_) override;
    size_t approximateSerializedSize() const noexcept override;
    bool operator==(const UserDefinedFunctionPayload & rhs) const override;
    std::string string() const override;
    UDFType type() const noexcept override { return UDFType::SQL; }
    bool isAggregation() const noexcept override { return false; }
    std::string_view sourceView() const noexcept override { return ""; }
    std::string
    createASTString(const std::string & name, const std::string & typed_arguments, const std::string & return_type) const override;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    /// query of create sql function
    std::string query;
};
using SQLUserDefinedFunctionPayloadPtr = std::shared_ptr<SQLUserDefinedFunctionPayload>;

SERDE class UserDefinedFunctionDescriptor
{
public:
    struct Argument
    {
        std::string name;
        std::string type;

        bool operator==(const Argument & rhs) const noexcept { return name == rhs.name && type == rhs.type; }
    };

    UserDefinedFunctionDescriptor() = default;

    UserDefinedFunctionDescriptor(const UserDefinedFunctionDescriptor &) = default;

    UserDefinedFunctionDescriptor(
        const std::vector<UserDefinedFunctionDescriptor::Argument> & arguments_,
        const std::string & name_,
        const std::string & return_type_,
        UserDefinedFunctionPayloadPtr payload_,
        int64_t create_timestamp_ms_,
        int64_t last_modify_timestamp_ms_,
        const std::string & created_by_,
        const std::string & last_modified_by_,
        uint32_t data_version_)
        : data_version(data_version_)
        , arguments(arguments_)
        , name(name_)
        , return_type(return_type_)
        , payload(std::move(payload_))
        , create_timestamp_ms(create_timestamp_ms_)
        , last_modify_timestamp_ms(last_modify_timestamp_ms_)
        , created_by(created_by_)
        , last_modified_by(last_modified_by_)
    {
        assert(payload);
    }


    void serialize(DB::WriteBuffer & wb, uint16_t version_) const
    {
        serializeHeader(wb, version_);
        payload->serialize(wb, Nulls::NullVersion);
    }

    void deserialize(DB::ReadBuffer & rb, uint16_t version_)
    {
        auto type = deserializeHeader(rb, version_);
        parsePayload(rb, type, Nulls::NullVersion);
    }

    size_t approximateSerializedSize() const noexcept
    {
        return sizeof(UserDefinedFunctionDescriptor) + payload->approximateSerializedSize();
    }

    bool operator==(const UserDefinedFunctionDescriptor & rhs) const noexcept
    {
        return data_version == rhs.data_version && arguments == rhs.arguments && name == rhs.name && return_type == rhs.return_type
            && *payload == *rhs.payload;
    }

    std::string string() const;
    std::string createASTString() const;

    auto type() const noexcept { return payload->type(); }

private:
    void serializeHeader(DB::WriteBuffer & wb, uint16_t version_) const;
    UDFType deserializeHeader(DB::ReadBuffer & rb, uint16_t version_);
    void parsePayload(DB::ReadBuffer & rb, UDFType type, uint16_t version_);

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 2;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    std::vector<Argument> arguments;

    std::string name;

    std::string return_type;

    UserDefinedFunctionPayloadPtr payload;

    /// Added in schema_version 2
    int64_t create_timestamp_ms = 0;
    int64_t last_modify_timestamp_ms = 0;
    std::string created_by;
    std::string last_modified_by;
};

using UserDefinedFunctionDescriptorPtr = std::shared_ptr<UserDefinedFunctionDescriptor>;
using UserDefinedFunctionDescriptorPtrs = std::vector<UserDefinedFunctionDescriptorPtr>;
using UserDefinedFunctionDescriptors = std::vector<UserDefinedFunctionDescriptor>;
}
