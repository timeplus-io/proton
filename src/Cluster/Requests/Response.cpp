#include <Cluster/Requests/Response.h>

#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/Requests/ChangeLogLevelResponse.h>
#include <Cluster/Requests/CreateAccessEntityResponse.h>
#include <Cluster/Requests/CreateAlertResponse.h>
#include <Cluster/Requests/CreateDatabaseResponse.h>
#include <Cluster/Requests/CreateDiskResponse.h>
#include <Cluster/Requests/CreateFormatSchemaResponse.h>
#include <Cluster/Requests/CreateStoragePolicyResponse.h>
#include <Cluster/Requests/CreateStreamResponse.h>
#include <Cluster/Requests/CreateTaskResponse.h>
#include <Cluster/Requests/CreateUserDefinedFunctionResponse.h>
#include <Cluster/Requests/DeleteAccessEntityResponse.h>
#include <Cluster/Requests/DeleteAlertResponse.h>
#include <Cluster/Requests/DeleteDatabaseResponse.h>
#include <Cluster/Requests/DeleteDiskResponse.h>
#include <Cluster/Requests/DeleteFormatSchemaResponse.h>
#include <Cluster/Requests/DeleteStoragePolicyResponse.h>
#include <Cluster/Requests/DeleteStreamResponse.h>
#include <Cluster/Requests/DeleteTaskResponse.h>
#include <Cluster/Requests/DeleteUserDefinedFunctionResponse.h>
#include <Cluster/Requests/DropFormatSchemaCacheResponse.h>
#include <Cluster/Requests/ErrorResponse.h>
#include <Cluster/Requests/GetAlertResponse.h>
#include <Cluster/Requests/GetDatabaseResponse.h>
#include <Cluster/Requests/GetFormatSchemaResponse.h>
#include <Cluster/Requests/GetStreamResponse.h>
#include <Cluster/Requests/GetTaskResponse.h>
#include <Cluster/Requests/GetUserDefinedFunctionResponse.h>
#include <Cluster/Requests/ListAccessEntitiesResponse.h>
#include <Cluster/Requests/ListAlertsResponse.h>
#include <Cluster/Requests/ListDatabasesResponse.h>
#include <Cluster/Requests/ListDisksResponse.h>
#include <Cluster/Requests/ListFormatSchemasRequest.h>
#include <Cluster/Requests/ListFormatSchemasResponse.h>
#include <Cluster/Requests/ListStoragePoliciesResponse.h>
#include <Cluster/Requests/ListStreamsResponse.h>
#include <Cluster/Requests/ListTasksResponse.h>
#include <Cluster/Requests/ListUserDefinedFunctionsResponse.h>
#include <Cluster/Requests/PipPythonPackageResponse.h>
#include <Cluster/Requests/RenameStreamResponse.h>
#include <Cluster/Requests/RequestHeader.h>
#include <Cluster/Requests/ResponseHeader.h>
#include <Cluster/Requests/UpdateAccessEntityResponse.h>
#include <Cluster/Requests/UpdateStreamSchemaResponse.h>
#include <Cluster/Requests/UpdateStreamSettingsResponse.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>

namespace DB::ErrorCodes
{
extern const int UNSUPPORTED;
}

namespace cluster
{
void Response::serializeWithHeader(DB::WriteBuffer & wb, const ResponseHeader & header) const
{
    header.serialize(wb);
    data().serialize(wb, data_version);
}

ByteVector Response::serializeWithHeader(const ResponseHeader & header) const
{
    auto approx_size = header.data().approximateSerializedSize() + data().approximateSerializedSize();
    ByteVector buffer(approx_size);

    {
        DB::WriteBufferFromVector<ByteVector> wb(buffer);
        serializeWithHeader(wb, header);
        wb.finalize();
    }

    return buffer;
}

std::string Response::serializeWithHeaderInString(const ResponseHeader & header) const
{
    auto approx_size = header.data().approximateSerializedSize() + data().approximateSerializedSize();
    std::string buffer;
    buffer.resize(approx_size);

    {
        DB::WriteBufferFromString wb(buffer);
        serializeWithHeader(wb, header);
        wb.finalize();
    }

    return buffer;
}

ByteVector Response::serializeWithSizeAndHeader(const ResponseHeader & header) const
{
    auto approx_size = header.data().approximateSerializedSize() + data().approximateSerializedSize();
    ByteVector buffer(approx_size);

    /// Write a dummy size first and fix it later
    uint32_t prefix_length = 0;

    {
        DB::WriteBufferFromVector<ByteVector> wb(buffer);

        DB::writeIntBinary(prefix_length, wb);

        serializeWithHeader(wb, header);
        wb.finalize();
    }

    /// Fix the prefix length when we know it exactly
    prefix_length = static_cast<uint32_t>(buffer.size() - sizeof(prefix_length));
    buffer.prefixWithLength(prefix_length);

    return buffer;
}

bool Response::hasPermanentError() const noexcept
{
    const auto & err = error();
    if (!err.hasError())
        return false;

    /// FIXME, check error code
    return false;
}

ResponsePtr Response::parse(protocol::OpCode opcode, uint16_t version, std::string_view data)
{
    DB::ReadBufferFromString rb(data);
    return parse(opcode, version, rb);
}

ResponsePtr Response::parse(protocol::OpCode opcode, uint16_t version, DB::ReadBuffer & rb)
{
    ResponsePtr response;

    switch (opcode)
    {
        case protocol::OpCode::Error:
            response = std::make_shared<ErrorResponse>(version);
            break;
        case protocol::OpCode::CreateStream:
            response = std::make_shared<CreateStreamResponse>(version);
            break;
        case protocol::OpCode::DeleteStream:
            response = std::make_shared<DeleteStreamResponse>(version);
            break;
        case protocol::OpCode::ListStreams:
            response = std::make_shared<ListStreamsResponse>(version);
            break;
        case protocol::OpCode::AlterStreamSettings:
            response = std::make_shared<UpdateStreamSettingsResponse>(version);
            break;
        case protocol::OpCode::AlterStreamSchema:
            response = std::make_shared<UpdateStreamSchemaResponse>(version);
            break;
        case protocol::OpCode::RenameStream:
            response = std::make_shared<RenameStreamResponse>(version);
            break;
        case protocol::OpCode::GetStream:
            response = std::make_shared<GetStreamResponse>(version);
            break;
        case protocol::OpCode::CreateUserDefinedFunction:
            response = std::make_shared<CreateUserDefinedFunctionResponse>(version);
            break;
        case protocol::OpCode::DeleteUserDefinedFunction:
            response = std::make_shared<DeleteUserDefinedFunctionResponse>(version);
            break;
        case protocol::OpCode::ListUserDefinedFunctions:
            response = std::make_shared<ListUserDefinedFunctionsResponse>(version);
            break;
        case protocol::OpCode::GetUserDefinedFunction:
            response = std::make_shared<GetUserDefinedFunctionResponse>(version);
            break;
        case protocol::OpCode::CreateFormatSchema:
            response = std::make_shared<CreateFormatSchemaResponse>(version);
            break;
        case protocol::OpCode::DeleteFormatSchema:
            response = std::make_shared<DeleteFormatSchemaResponse>(version);
            break;
        case protocol::OpCode::ListFormatSchemas:
            response = std::make_shared<ListFormatSchemasResponse>(version);
            break;
        case protocol::OpCode::GetFormatSchema:
            response = std::make_shared<GetFormatSchemaResponse>(version);
            break;
        case protocol::OpCode::DropFormatSchemaCache:
            response = std::make_shared<DropFormatSchemaCacheResponse>(version);
            break;
        case protocol::OpCode::ChangeLogLevel:
            response = std::make_shared<ChangeLogLevelResponse>(version);
            break;
        case protocol::OpCode::PipPythonPackage:
            response = std::make_shared<PipPythonPackageResponse>(version);
            break;
        case protocol::OpCode::CreateAccessEntity:
            response = std::make_shared<CreateAccessEntityResponse>(version);
            break;
        case protocol::OpCode::DeleteAccessEntity:
            response = std::make_shared<DeleteAccessEntityResponse>(version);
            break;
        case protocol::OpCode::AlterAccessEntity:
            response = std::make_shared<UpdateAccessEntityResponse>(version);
            break;
        case protocol::OpCode::ListAccessEntities:
            response = std::make_shared<ListAccessEntitiesResponse>(version);
            break;
        case protocol::OpCode::CreateDatabase:
            response = std::make_shared<CreateDatabaseResponse>(version);
            break;
        case protocol::OpCode::DeleteDatabase:
            response = std::make_shared<DeleteDatabaseResponse>(version);
            break;
        case protocol::OpCode::GetDatabase:
            response = std::make_shared<GetDatabaseResponse>(version);
            break;
        case protocol::OpCode::ListDatabases:
            response = std::make_shared<ListDatabasesResponse>(version);
            break;
        case protocol::OpCode::CreateDisk:
            response = std::make_shared<CreateDiskResponse>(version);
            break;
        case protocol::OpCode::DeleteDisk:
            response = std::make_shared<DeleteDiskResponse>(version);
            break;
        case protocol::OpCode::ListDisks:
            response = std::make_shared<ListDisksResponse>(version);
            break;
        case protocol::OpCode::CreateStoragePolicy:
            response = std::make_shared<CreateStoragePolicyResponse>(version);
            break;
        case protocol::OpCode::DeleteStoragePolicy:
            response = std::make_shared<DeleteStoragePolicyResponse>(version);
            break;
        case protocol::OpCode::ListStoragePolicies:
            response = std::make_shared<ListStoragePoliciesResponse>(version);
            break;
        case protocol::OpCode::CreateAlert:
            response = std::make_shared<CreateAlertResponse>(version);
            break;
        case protocol::OpCode::DeleteAlert:
            response = std::make_shared<DeleteAlertResponse>(version);
            break;
        case protocol::OpCode::GetAlert:
            response = std::make_shared<GetAlertResponse>(version);
            break;
        case protocol::OpCode::ListAlerts:
            response = std::make_shared<ListAlertsResponse>(version);
            break;
        case protocol::OpCode::CreateTask:
            response = std::make_shared<CreateTaskResponse>(version);
            break;
        case protocol::OpCode::DeleteTask:
            response = std::make_shared<DeleteTaskResponse>(version);
            break;
        case protocol::OpCode::GetTask:
            response = std::make_shared<GetTaskResponse>(version);
            break;
        case protocol::OpCode::ListTasks:
            response = std::make_shared<ListTasksResponse>(version);
            break;
        default:
            /// FIXME, when we support more response, add it here
            /// For now let's assert to make debug easier
            /// assert(false);
            return nullptr;
    }

    /// Parse the response data part
    response->deserialize(rb);

    return response;
}

/// Parse response from read data buffer. The data buffer is expected to hold both ResponseHeader and the response payload
ResponsePtr Response::parseWithHeader(std::string_view data, ResponseHeader & header)
{
    /// Response header + Response
    DB::ReadBufferFromString rb(data);
    header = ResponseHeader::parse(rb);

    auto offset = rb.offset();
    assert(offset < data.size());
    std::string_view response_data(data.data() + offset, data.size() - offset);

    return Response::parse(header.responseOpCode(), header.responseVersion(), response_data);
}

}
