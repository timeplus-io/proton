#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>

#include <ranges>

namespace DB::ErrorCodes
{
extern const int INVALID_DATA;
extern const int UNSUPPORTED;
}

template <>
struct fmt::formatter<cluster::protocol::RemoteUserDefinedFunctionPayload::AuthContext>
{
    constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const cluster::protocol::RemoteUserDefinedFunctionPayload::AuthContext & arg, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "(key_name={}, key_value={})", arg.key_name, arg.key_value);
    }
};

template <>
struct fmt::formatter<cluster::protocol::UserDefinedFunctionDescriptor::Argument>
{
    constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const cluster::protocol::UserDefinedFunctionDescriptor::Argument & arg, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "(name={}, type={})", arg.name, arg.type);
    }
};

namespace cluster::protocol
{

namespace
{
std::string doCreateASTString(
    const std::string & name,
    const std::string & typed_arguments,
    const std::string & return_type,
    const UserDefinedFunctionPayload & payload)
{
    String language{magic_enum::enum_name(payload.type())};
    boost::to_upper(language);

    return fmt::format(
        "CREATE OR REPLACE {}FUNCTION {}({}) RETURNS {} LANGUAGE {} AS $${}$$",
        payload.isAggregation() ? "AGGREGATE " : "",
        name,
        typed_arguments,
        return_type,
        language,
        payload.sourceView());
}

}

void ExecutableUserDefinedFunctionPayload::serialize(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);
    DB::writeStringBinary(command, wb);

    DB::writeVarUInt(static_cast<uint64_t>(command_arguments.size()), wb);
    for (const auto & command_argument : command_arguments)
        DB::writeStringBinary(command_argument, wb);

    DB::writeStringBinary(format, wb);
    DB::writeVarUInt(command_termination_timeout_seconds, wb);
    DB::writeVarUInt(command_read_timeout_milliseconds, wb);
    DB::writeVarUInt(command_write_timeout_milliseconds, wb);
    DB::writeVarUInt(max_command_execution_time_seconds, wb);
    DB::writeVarUInt(pool_size, wb);
    DB::writeBinary(is_executable_pool, wb);
    DB::writeBinary(send_chunk_header, wb);
    DB::writeBinary(execute_direct, wb);
}

void ExecutableUserDefinedFunctionPayload::deserialize(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readStringBinary(command, rb);

    uint64_t command_arguments_size = 0;
    DB::readVarUInt(command_arguments_size, rb);
    command_arguments.reserve(command_arguments_size);
    for (uint64_t i = 0; i < command_arguments_size; ++i)
    {
        std::string command_argument;
        DB::readStringBinary(command_argument, rb);
        command_arguments.push_back(std::move(command_argument));
    }

    DB::readStringBinary(format, rb);
    DB::readVarUInt(command_termination_timeout_seconds, rb);
    DB::readVarUInt(command_read_timeout_milliseconds, rb);
    DB::readVarUInt(command_write_timeout_milliseconds, rb);
    DB::readVarUInt(max_command_execution_time_seconds, rb);
    DB::readVarUInt(pool_size, rb);
    DB::readBinary(is_executable_pool, rb);
    DB::readBinary(send_chunk_header, rb);
    DB::readBinary(execute_direct, rb);
}

size_t ExecutableUserDefinedFunctionPayload::approximateSerializedSize() const noexcept
{
    size_t size = sizeof(schema_version) + sizeof(data_version) + approximateSerializedSizeOf(command) + approximateSerializedSizeOf(format)
        + sizeof(command_termination_timeout_seconds) + sizeof(command_read_timeout_milliseconds)
        + sizeof(command_write_timeout_milliseconds) + sizeof(max_command_execution_time_seconds) + sizeof(pool_size)
        + sizeof(is_executable_pool) + sizeof(send_chunk_header) + sizeof(execute_direct);

    size += sizeof(command_arguments.size());
    for (const auto & command_argument : command_arguments)
        size += approximateSerializedSizeOf(command_argument);
    return size;
}

bool ExecutableUserDefinedFunctionPayload::operator==(const UserDefinedFunctionPayload & rhs) const
{
    if (type() != rhs.type())
        return false;

    auto * other = dynamic_cast<const ExecutableUserDefinedFunctionPayload *>(&rhs);

    return data_version == other->data_version && command == other->command && command_arguments == other->command_arguments
        && format == other->format && command_termination_timeout_seconds == other->command_termination_timeout_seconds
        && command_read_timeout_milliseconds == other->command_read_timeout_milliseconds
        && command_write_timeout_milliseconds == other->command_write_timeout_milliseconds
        && max_command_execution_time_seconds == other->max_command_execution_time_seconds && pool_size == other->pool_size
        && is_executable_pool == other->is_executable_pool && send_chunk_header == other->send_chunk_header
        && execute_direct == other->execute_direct;
}

std::string ExecutableUserDefinedFunctionPayload::string() const
{
    return fmt::format(
        "version={}, command={}, command_arguments=[{}], format={}, command_termination_timeout_seconds={}, "
        "command_read_timeout_milliseconds={}, command_write_timeout_milliseconds={}, max_command_execution_time_seconds={}, pool_size={}, "
        "is_executable_pool={}, send_chunk_header={}, execute_direct={}",
        data_version,
        command,
        fmt::join(command_arguments, ", "),
        format,
        command_termination_timeout_seconds,
        command_read_timeout_milliseconds,
        command_write_timeout_milliseconds,
        max_command_execution_time_seconds,
        pool_size,
        is_executable_pool,
        send_chunk_header,
        execute_direct);
}

std::string ExecutableUserDefinedFunctionPayload::createASTString(
    const std::string & name, const std::string & typed_arguments, const std::string & return_type) const
{
    return doCreateASTString(name, typed_arguments, return_type, *this);
}

void RemoteUserDefinedFunctionPayload::serialize(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeVarUInt(command_read_timeout_milliseconds, wb);
    DB::writeStringBinary(url, wb);
    cluster::serializeEnum(auth_method, wb);
    DB::writeStringBinary(auth_context.key_name, wb);
    DB::writeStringBinary(auth_context.key_value, wb);

    /// Added in schema_version 2
    DB::writeVarUInt(command_execution_timeout_milliseconds, wb);
}

void RemoteUserDefinedFunctionPayload::deserialize(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    auto source_schema_version = static_cast<uint32_t>(schema_data_version >> 32);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readVarUInt(command_read_timeout_milliseconds, rb);
    DB::readStringBinary(url, rb);
    auth_method = cluster::deserializeEnum<AuthMethod>(rb);
    DB::readStringBinary(auth_context.key_name, rb);
    DB::readStringBinary(auth_context.key_value, rb);

    if (source_schema_version >= 2)
        DB::readVarUInt(command_execution_timeout_milliseconds, rb);
}

size_t RemoteUserDefinedFunctionPayload::approximateSerializedSize() const noexcept
{
    size_t size = sizeof(schema_version) + sizeof(data_version) + sizeof(command_read_timeout_milliseconds)
        + approximateSerializedSizeOf(url) + sizeof(auth_method) + approximateSerializedSizeOf(auth_context.key_name)
        + approximateSerializedSizeOf(auth_context.key_value) + sizeof(command_execution_timeout_milliseconds);
    return size;
}

bool RemoteUserDefinedFunctionPayload::operator==(const UserDefinedFunctionPayload & rhs) const
{
    if (type() != rhs.type())
        return false;

    auto * other = dynamic_cast<const RemoteUserDefinedFunctionPayload *>(&rhs);

    return data_version == other->data_version && command_read_timeout_milliseconds == other->command_read_timeout_milliseconds
        && url == other->url && auth_method == other->auth_method && auth_context.key_name == other->auth_context.key_name
        && command_execution_timeout_milliseconds == other->command_execution_timeout_milliseconds
        && auth_context.key_value == other->auth_context.key_value;
}

std::string RemoteUserDefinedFunctionPayload::string() const
{
    return fmt::format(
        "version={}, command_read_timeout_milliseconds={}, command_execution_timeout_milliseconds={}, url='{}', auth_method={}, "
        "auth_context={}",
        data_version,
        command_read_timeout_milliseconds,
        command_execution_timeout_milliseconds,
        url,
        magic_enum::enum_name(auth_method),
        auth_context);
}

std::string RemoteUserDefinedFunctionPayload::createASTString(
    const std::string & name, const std::string & typed_arguments, const std::string & return_type) const
{
    DB::WriteBufferFromOwnString buf;

    buf << fmt::format("CREATE OR REPLACE REMOTE FUNCTION {}({}) RETURNS {}\n", name, typed_arguments, return_type);

    const auto * auth_method_s = auth_method == RemoteUserDefinedFunctionPayload::AuthMethod::AuthHeader ? "auth_header" : "none";
    buf << fmt::format("URL '{}'\n", url);
    buf << fmt::format("AUTH_METHOD '{}'\n", auth_method_s);

    if (auth_method == RemoteUserDefinedFunctionPayload::AuthMethod::AuthHeader)
    {
        buf << fmt::format("AUTH_HEADER '{}'\n", auth_context.key_name);
        buf << fmt::format("AUTH_KEY '{}'\n", auth_context.key_value);
    }
    buf << fmt::format("EXECUTION_TIMEOUT {}\n", command_execution_timeout_milliseconds);

    return buf.str();
}

void JavaScriptUserDefinedFunctionPayload::serialize(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);
    DB::writeBinary(is_aggregation, wb);
    DB::writeStringBinary(source, wb);
}

void JavaScriptUserDefinedFunctionPayload::deserialize(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readBinary(is_aggregation, rb);
    DB::readStringBinary(source, rb);
}

size_t JavaScriptUserDefinedFunctionPayload::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(data_version) + sizeof(is_aggregation) + approximateSerializedSizeOf(source);
}

bool JavaScriptUserDefinedFunctionPayload::operator==(const UserDefinedFunctionPayload & rhs) const
{
    if (type() != rhs.type())
        return false;

    auto * other = dynamic_cast<const JavaScriptUserDefinedFunctionPayload *>(&rhs);
    return data_version == other->data_version && source == other->source && is_aggregation == other->is_aggregation;
}

std::string JavaScriptUserDefinedFunctionPayload::string() const
{
    return fmt::format("version={}, is_aggregation={}, source=`{}`", data_version, is_aggregation, source);
}

std::string JavaScriptUserDefinedFunctionPayload::createASTString(
    const std::string & name, const std::string & typed_arguments, const std::string & return_type) const
{
    return doCreateASTString(name, typed_arguments, return_type, *this);
}

void PythonUserDefinedFunctionPayload::serialize(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    /// Wire/back-compat: only emit the schema_version-2 layout (the three trailing
    /// init-hook strings) when an init hook is actually configured. Otherwise fall
    /// back to the v1 layout, producing bytes a pre-v2 binary reads exactly.
    ///
    /// This matters because the payload is serialized inline (no length prefix) and
    /// is NOT the last field on the wire: CreateUserDefinedFunctionRequestData writes
    /// exists_op/initiator/timeout_ms right after the descriptor. If we always wrote
    /// the v2 strings, a pre-v2 reader would stop after `source` and then decode those
    /// trailing request fields from the wrong offset (corruption). Emitting v1 when the
    /// fields are empty — every UDF created before this feature, and any that does not
    /// use it — keeps mixed-version clusters safe; only UDFs that actually use the init
    /// hook require a fully-upgraded cluster, which is inherent to an additive feature.
    const bool has_init_hook
        = !init_function_name.empty() || !init_function_parameters.empty() || !named_collection.empty();
    const uint32_t effective_schema_version = has_init_hook ? schema_version : 1;

    uint64_t schema_data_version = (static_cast<uint64_t>(effective_schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeBinary(is_aggregation, wb);
    DB::writeBinary(using_numpy, wb);
    DB::writeStringBinary(source, wb);

    if (effective_schema_version >= 2)
    {
        /// Added in schema_version 2
        DB::writeStringBinary(init_function_name, wb);
        DB::writeStringBinary(init_function_parameters, wb);
        DB::writeStringBinary(named_collection, wb);
    }
}

void PythonUserDefinedFunctionPayload::deserialize(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    auto source_schema_version = static_cast<uint32_t>(schema_data_version >> 32);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readBinary(is_aggregation, rb);
    DB::readBinary(using_numpy, rb);
    DB::readStringBinary(source, rb);

    if (source_schema_version >= 2)
    {
        DB::readStringBinary(init_function_name, rb);
        DB::readStringBinary(init_function_parameters, rb);
        DB::readStringBinary(named_collection, rb);
    }
}

size_t PythonUserDefinedFunctionPayload::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(data_version) + sizeof(is_aggregation) + sizeof(using_numpy)
        + approximateSerializedSizeOf(source) + approximateSerializedSizeOf(init_function_name)
        + approximateSerializedSizeOf(init_function_parameters) + approximateSerializedSizeOf(named_collection);
}

bool PythonUserDefinedFunctionPayload::operator==(const UserDefinedFunctionPayload & rhs) const
{
    if (type() != rhs.type())
        return false;

    auto * other = dynamic_cast<const PythonUserDefinedFunctionPayload *>(&rhs);
    return data_version == other->data_version && source == other->source && is_aggregation == other->is_aggregation
        && using_numpy == other->using_numpy && init_function_name == other->init_function_name
        && init_function_parameters == other->init_function_parameters && named_collection == other->named_collection;
}

std::string PythonUserDefinedFunctionPayload::string() const
{
    return fmt::format(
        "version={}, is_aggregation={}, using_numpy={}, init_function_name={}, named_collection={}, source=`{}`",
        data_version,
        is_aggregation,
        using_numpy,
        init_function_name,
        named_collection,
        source);
}

std::string PythonUserDefinedFunctionPayload::createASTString(
    const std::string & name, const std::string & typed_arguments, const std::string & return_type) const
{
    auto ast = doCreateASTString(name, typed_arguments, return_type, *this);
    /// parameters/named_collection without init_function_name is rejected at creation
    /// time for all entry points (see Streaming::createUserDefinedFunctionRequest)
    if (init_function_name.empty())
        return ast;

    ast += fmt::format("\nSETTINGS init_function_name = {}", DB::quoteString(init_function_name));
    if (!named_collection.empty())
        ast += fmt::format(", named_collection = {}", DB::quoteString(named_collection));
    if (!init_function_parameters.empty())
        ast += fmt::format(", init_function_parameters = {}", DB::quoteString(init_function_parameters));
    return ast;
}

void SQLUserDefinedFunctionPayload::serialize(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    DB::writeStringBinary(query, wb);
}

void SQLUserDefinedFunctionPayload::deserialize(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);

    DB::readStringBinary(query, rb);
}

size_t SQLUserDefinedFunctionPayload::approximateSerializedSize() const noexcept
{
    return sizeof(schema_version) + sizeof(data_version) + approximateSerializedSizeOf(query);
}

bool SQLUserDefinedFunctionPayload::operator==(const UserDefinedFunctionPayload & rhs) const
{
    if (type() != rhs.type())
        return false;

    auto * other = dynamic_cast<const SQLUserDefinedFunctionPayload *>(&rhs);
    return data_version == other->data_version && query == other->query;
}

std::string SQLUserDefinedFunctionPayload::string() const
{
    return fmt::format("version={}, query=`{}`", data_version, query);
}

std::string SQLUserDefinedFunctionPayload::createASTString(
    [[maybe_unused]] const std::string & name,
    [[maybe_unused]] const std::string & typed_arguments,
    [[maybe_unused]] const std::string & return_type) const
{
    return query;
}

std::string UserDefinedFunctionDescriptor::string() const
{
    return fmt::format(
        "version={}, type={}, arguments=[{}], name={}, return_type={}, payload=({})",
        data_version,
        magic_enum::enum_name(payload->type()),
        fmt::join(arguments, ", "),
        name,
        return_type,
        payload->string());
}

std::string UserDefinedFunctionDescriptor::createASTString() const
{
    DB::WriteBufferFromOwnString buf;

    auto formatted = arguments | std::views::transform([](const auto & arg) { return fmt::format("{} {}", arg.name, arg.type); });
    std::string typed_arguments = fmt::format("{}", fmt::join(formatted, ", "));
    return payload->createASTString(name, typed_arguments, return_type);
}

void UserDefinedFunctionDescriptor::serializeHeader(DB::WriteBuffer & wb, uint16_t /* version_ */) const
{
    uint64_t schema_data_version = (static_cast<uint64_t>(schema_version) << 32) + data_version;
    DB::writeVarUInt(schema_data_version, wb);

    cluster::serializeEnum(payload->type(), wb);

    DB::writeVarUInt(static_cast<uint64_t>(arguments.size()), wb);
    for (const auto & argument : arguments)
    {
        DB::writeStringBinary(argument.name, wb);
        DB::writeStringBinary(argument.type, wb);
    }

    DB::writeStringBinary(name, wb);
    DB::writeStringBinary(return_type, wb);

    if (schema_version >= 2)
    {
        DB::writeVarInt(create_timestamp_ms, wb);
        DB::writeVarInt(last_modify_timestamp_ms, wb);
        DB::writeStringBinary(created_by, wb);
        DB::writeStringBinary(last_modified_by, wb);
    }
}

UDFType UserDefinedFunctionDescriptor::deserializeHeader(DB::ReadBuffer & rb, uint16_t /* version_ */)
{
    uint64_t schema_data_version;
    DB::readVarUInt(schema_data_version, rb);
    data_version = static_cast<uint32_t>(schema_data_version & 0xFFFF'FFFF);
    auto source_schema_version = static_cast<uint32_t>(schema_data_version >> 32);

    auto type = cluster::deserializeEnum<UDFType>(rb);

    uint64_t arg_size = 0;
    DB::readVarUInt(arg_size, rb);

    arguments.reserve(arg_size);
    for (uint64_t i = 0; i < arg_size; ++i)
    {
        Argument argument;
        DB::readStringBinary(argument.name, rb);
        DB::readStringBinary(argument.type, rb);
        arguments.push_back(std::move(argument));
    }

    DB::readStringBinary(name, rb);
    DB::readStringBinary(return_type, rb);

    if (source_schema_version >= 2)
    {
        DB::readVarInt(create_timestamp_ms, rb);
        DB::readVarInt(last_modify_timestamp_ms, rb);
        DB::readStringBinary(created_by, rb);
        DB::readStringBinary(last_modified_by, rb);
    }

    return type;
}

void UserDefinedFunctionDescriptor::parsePayload(DB::ReadBuffer & rb, UDFType type, uint16_t version_)
{
    switch (type)
    {
        case UDFType::Executable:
        {
            payload = std::make_shared<ExecutableUserDefinedFunctionPayload>();
            payload->deserialize(rb, version_);
            break;
        }
        case UDFType::Remote:
        {
            payload = std::make_shared<RemoteUserDefinedFunctionPayload>();
            payload->deserialize(rb, version_);
            break;
        }
        case UDFType::Javascript:
        {
            payload = std::make_shared<JavaScriptUserDefinedFunctionPayload>();
            payload->deserialize(rb, version_);
            break;
        }
        case UDFType::Python:
        {
#if USE_PYTHON_UDF
            payload = std::make_shared<PythonUserDefinedFunctionPayload>();
            payload->deserialize(rb, version_);
            break;
#else
            throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "Python UDF is not supported");
#endif
        }
        case UDFType::SQL:
        {
            payload = std::make_shared<SQLUserDefinedFunctionPayload>();
            payload->deserialize(rb, version_);
            break;
        }
    }
}
}
