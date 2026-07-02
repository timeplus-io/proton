#include <Cluster/Common/serde.h>
#include <Cluster/Requests/AlterTaskRequest.h>
#include <Cluster/Requests/AssignMaterializedViewRequest.h>
#include <Cluster/Requests/ChangeLogLevelRequest.h>
#include <Cluster/Requests/CreateAccessEntityRequest.h>
#include <Cluster/Requests/CreateAlertRequest.h>
#include <Cluster/Requests/CreateDatabaseRequest.h>
#include <Cluster/Requests/CreateDiskRequest.h>
#include <Cluster/Requests/CreateFormatSchemaRequest.h>
#include <Cluster/Requests/CreateNamedCollectionRequest.h>
#include <Cluster/Requests/CreateStoragePolicyRequest.h>
#include <Cluster/Requests/CreateStreamRequest.h>
#include <Cluster/Requests/CreateTaskRequest.h>
#include <Cluster/Requests/CreateUserDefinedFunctionRequest.h>
#include <Cluster/Requests/DeleteAccessEntityRequest.h>
#include <Cluster/Requests/DeleteAlertRequest.h>
#include <Cluster/Requests/DeleteDatabaseRequest.h>
#include <Cluster/Requests/DeleteDiskRequest.h>
#include <Cluster/Requests/DeleteFormatSchemaRequest.h>
#include <Cluster/Requests/DeleteNamedCollectionRequest.h>
#include <Cluster/Requests/DeleteStoragePolicyRequest.h>
#include <Cluster/Requests/DeleteStreamRequest.h>
#include <Cluster/Requests/DeleteTaskRequest.h>
#include <Cluster/Requests/DeleteUserDefinedFunctionRequest.h>
#include <Cluster/Requests/DropFormatSchemaCacheRequest.h>
#include <Cluster/Requests/GetAlertRequest.h>
#include <Cluster/Requests/GetDatabaseRequest.h>
#include <Cluster/Requests/GetFormatSchemaRequest.h>
#include <Cluster/Requests/GetNamedCollectionRequest.h>
#include <Cluster/Requests/GetStreamRequest.h>
#include <Cluster/Requests/GetTaskRequest.h>
#include <Cluster/Requests/GetUserDefinedFunctionRequest.h>
#include <Cluster/Requests/ListAccessEntitiesRequest.h>
#include <Cluster/Requests/ListAlertsRequest.h>
#include <Cluster/Requests/ListDatabasesRequest.h>
#include <Cluster/Requests/ListDisksRequest.h>
#include <Cluster/Requests/ListFormatSchemasRequest.h>
#include <Cluster/Requests/ListNamedCollectionsRequest.h>
#include <Cluster/Requests/ListStoragePoliciesRequest.h>
#include <Cluster/Requests/ListStreamsRequest.h>
#include <Cluster/Requests/ListTasksRequest.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <Cluster/Requests/PipPythonPackageRequest.h>
#include <Cluster/Requests/RenameStreamRequest.h>
#include <Cluster/Requests/Request.h>
#include <Cluster/Requests/UpdateAccessEntityRequest.h>
#include <Cluster/Requests/UpdateStreamSchemaRequest.h>
#include <Cluster/Requests/UpdateStreamSettingsRequest.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED;
}

namespace cluster
{

/// Serializes request header and body without prefixing with size
void Request::serializeWithHeader(DB::WriteBuffer & wb, const RequestHeader & header) const
{
    auto request_opcode = data().opCode();

    if (header.requestOpCode() != request_opcode)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Could not build request '{}' with header opcode '{}'",
            magic_enum::enum_name(request_opcode),
            magic_enum::enum_name(header.opCode()));

    if (header.requestVersion() != data_version)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS, "Could not build request version '{}' with header version '{}'", data_version, header.version());

    header.serialize(wb);
    data().serialize(wb, data_version);
}

ByteVector Request::serializeWithHeader(const RequestHeader & header) const
{
    auto approx_size = static_cast<uint64_t>(1.15 * (header.data().approximateSerializedSize()) + data().approximateSerializedSize());
    ByteVector buffer(approx_size);

    DB::WriteBufferFromVector<ByteVector> wb(buffer);
    serializeWithHeader(wb, header);
    wb.finalize();

    return buffer;
}

ByteVector Request::serializeWithSizeAndHeader(const RequestHeader & header) const
{
    auto approx_size = static_cast<uint64_t>(1.15 * (header.data().approximateSerializedSize()) + data().approximateSerializedSize());
    ByteVector buffer(approx_size);

    DB::WriteBufferFromVector<ByteVector> wb(buffer);
    /// Write a dummy size first and fix it later
    uint32_t prefix_length = 0;
    DB::writeIntBinary(prefix_length, wb);

    serializeWithHeader(wb, header);
    wb.finalize();

    /// Fix the prefix length when we know it exactly
    prefix_length = static_cast<uint32_t>(buffer.size() - sizeof(prefix_length));
    buffer.prefixWithLength(prefix_length);

    return buffer;
}

RequestPtr Request::parse(protocol::OpCode request_opcode, uint16_t request_version, DB::ReadBuffer & rb)
{
    RequestPtr request;

    switch (request_opcode)
    {
        case protocol::OpCode::CreateStream:
            request = std::make_shared<CreateStreamRequest>(request_version);
            break;
        case protocol::OpCode::AlterStreamSettings:
            request = std::make_shared<UpdateStreamSettingsRequest>(request_version);
            break;
        case protocol::OpCode::AlterStreamSchema:
            request = std::make_shared<UpdateStreamSchemaRequest>(request_version);
            break;
        case protocol::OpCode::DeleteStream:
            request = std::make_shared<DeleteStreamRequest>(request_version);
            break;
        case protocol::OpCode::RenameStream:
            request = std::make_shared<RenameStreamRequest>(request_version);
            break;
        case protocol::OpCode::ListStreams:
            request = std::make_shared<ListStreamsRequest>(request_version);
            break;
        case protocol::OpCode::GetStream:
            request = std::make_shared<GetStreamRequest>(request_version);
            break;
        case protocol::OpCode::CreateUserDefinedFunction:
            request = std::make_shared<CreateUserDefinedFunctionRequest>(request_version);
            break;
        case protocol::OpCode::DeleteUserDefinedFunction:
            request = std::make_shared<DeleteUserDefinedFunctionRequest>(request_version);
            break;
        case protocol::OpCode::ListUserDefinedFunctions:
            request = std::make_shared<ListUserDefinedFunctionsRequest>(request_version);
            break;
        case protocol::OpCode::GetUserDefinedFunction:
            request = std::make_shared<GetUserDefinedFunctionRequest>(request_version);
            break;
        case protocol::OpCode::CreateFormatSchema:
            request = std::make_shared<CreateFormatSchemaRequest>(request_version);
            break;
        case protocol::OpCode::DeleteFormatSchema:
            request = std::make_shared<DeleteFormatSchemaRequest>(request_version);
            break;
        case protocol::OpCode::ListFormatSchemas:
            request = std::make_shared<ListFormatSchemasRequest>(request_version);
            break;
        case protocol::OpCode::GetFormatSchema:
            request = std::make_shared<GetFormatSchemaRequest>(request_version);
            break;
        case protocol::OpCode::DropFormatSchemaCache:
            request = std::make_shared<DropFormatSchemaCacheRequest>(request_version);
            break;
        case protocol::OpCode::ChangeLogLevel:
            request = std::make_shared<ChangeLogLevelRequest>(request_version);
            break;
        case protocol::OpCode::PipPythonPackage:
            request = std::make_shared<PipPythonPackageRequest>(request_version);
            break;
        case protocol::OpCode::CreateAccessEntity:
            request = std::make_shared<CreateAccessEntityRequest>(request_version);
            break;
        case protocol::OpCode::DeleteAccessEntity:
            request = std::make_shared<DeleteAccessEntityRequest>(request_version);
            break;
        case protocol::OpCode::AlterAccessEntity:
            request = std::make_shared<UpdateAccessEntityRequest>(request_version);
            break;
        case protocol::OpCode::ListAccessEntities:
            request = std::make_shared<ListAccessEntitiesRequest>(request_version);
            break;
        case protocol::OpCode::CreateDatabase:
            request = std::make_shared<CreateDatabaseRequest>(request_version);
            break;
        case protocol::OpCode::DeleteDatabase:
            request = std::make_shared<DeleteDatabaseRequest>(request_version);
            break;
        case protocol::OpCode::ListDatabases:
            request = std::make_shared<ListDatabasesRequest>(request_version);
            break;
        case protocol::OpCode::GetDatabase:
            request = std::make_shared<GetDatabaseRequest>(request_version);
            break;
        case protocol::OpCode::CreateDisk:
            request = std::make_shared<CreateDiskRequest>(request_version);
            break;
        case protocol::OpCode::DeleteDisk:
            request = std::make_shared<DeleteDiskRequest>(request_version);
            break;
        case protocol::OpCode::ListDisks:
            request = std::make_shared<ListDisksRequest>(request_version);
            break;
        case protocol::OpCode::CreateStoragePolicy:
            request = std::make_shared<CreateStoragePolicyRequest>(request_version);
            break;
        case protocol::OpCode::DeleteStoragePolicy:
            request = std::make_shared<DeleteStoragePolicyRequest>(request_version);
            break;
        case protocol::OpCode::ListStoragePolicies:
            request = std::make_shared<ListStoragePoliciesRequest>(request_version);
            break;
        case protocol::OpCode::AssignMaterializedView:
            request = std::make_shared<AssignMaterializedViewRequest>(request_version);
            break;
        case protocol::OpCode::CreateAlert:
            request = std::make_shared<CreateAlertRequest>(request_version);
            break;
        case protocol::OpCode::DeleteAlert:
            request = std::make_shared<DeleteAlertRequest>(request_version);
            break;
        case protocol::OpCode::ListAlerts:
            request = std::make_shared<ListAlertsRequest>(request_version);
            break;
        case protocol::OpCode::GetAlert:
            request = std::make_shared<GetAlertRequest>(request_version);
            break;
        case protocol::OpCode::CreateTask:
            request = std::make_shared<CreateTaskRequest>(request_version);
            break;
        case protocol::OpCode::DeleteTask:
            request = std::make_shared<DeleteTaskRequest>(request_version);
            break;
        case protocol::OpCode::AlterTask:
            request = std::make_shared<AlterTaskRequest>(request_version);
            break;
        case protocol::OpCode::ListTasks:
            request = std::make_shared<ListTasksRequest>(request_version);
            break;
        case protocol::OpCode::GetTask:
            request = std::make_shared<GetTaskRequest>(request_version);
            break;
        case protocol::OpCode::CreateNamedCollection:
            request = std::make_shared<CreateNamedCollectionRequest>(request_version);
            break;
        case protocol::OpCode::DeleteNamedCollection:
            request = std::make_shared<DeleteNamedCollectionRequest>(request_version);
            break;
        case protocol::OpCode::ListNamedCollections:
            request = std::make_shared<ListNamedCollectionsRequest>(request_version);
            break;
        case protocol::OpCode::GetNamedCollection:
            request = std::make_shared<GetNamedCollectionRequest>(request_version);
            break;
        default:
            /// FIXME, when we support more request, add it here
            /// For now let's assert to make debug easier
            /// chassert(false);
            return nullptr;
    }

    /// Parse the request data part
    request->deserialize(rb);

    return request;
}

RequestPtr Request::parse(protocol::OpCode request_opcode, uint16_t request_version, std::string_view data)
{
    DB::ReadBufferFromString rb(data);
    return parse(request_opcode, request_version, rb);
}

RequestAndSize Request::parseWithHeader(std::string_view data, RequestHeader & header)
{
    /// Request header + Request
    DB::ReadBufferFromString rb(data);
    header = RequestHeader::parse(rb);

    auto offset = rb.offset();
    assert(offset < data.size());
    std::string_view request_data(data.data() + offset, data.size() - offset);

    if (auto req = Request::parse(header.requestOpCode(), header.requestVersion(), request_data); req)
        return RequestAndSize(std::move(req), static_cast<uint32_t>(request_data.size()));

    return {};
}

}
