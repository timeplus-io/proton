#pragma once

#include <functional>
#include <optional>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <Common/logger_useful.h>
#include <Poco/Format.h>
#include <Poco/RegularExpression.h>
#include <Poco/Net/StreamSocket.h>
#include "Types.h"
#include <unordered_map>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
}


namespace PostgreSQLProtocol
{

namespace Messaging
{

enum class FrontMessageType : Int32
{
// first message types
    CANCEL_REQUEST = 80877102,
    SSL_REQUEST = 80877103,
    GSSENC_REQUEST = 80877104,

// other front message types
    PASSWORD_MESSAGE = 'p',
    QUERY = 'Q',
    TERMINATE = 'X',
    PARSE = 'P',
    BIND = 'B',
    DESCRIBE = 'D',
    EXECUTE = 'E',
    SYNC = 'S',
    FLUSH = 'H',
    CLOSE = 'C',
};

enum class MessageType : Int32
{
// common
    ERROR_RESPONSE = 0,
    CANCEL_REQUEST = 1,
    COMMAND_COMPLETE = 2,
    NOTICE_RESPONSE = 3,
    NOTIFICATION_RESPONSE = 4,
    PARAMETER_STATUS = 5,
    READY_FOR_QUERY = 6,
    SYNC = 7,
    TERMINATE = 8,

// start up and authentication
    AUTHENTICATION_OK = 30,
    AUTHENTICATION_KERBEROS_V5 = 31,
    AUTHENTICATION_CLEARTEXT_PASSWORD = 32,
    AUTHENTICATION_MD5_PASSWORD = 33,
    AUTHENTICATION_SCM_CREDENTIAL = 34,
    AUTHENTICATION_GSS = 35,
    AUTHENTICATION_SSPI = 36,
    AUTHENTICATION_GSS_CONTINUE = 37,
    AUTHENTICATION_SASL = 38,
    AUTHENTICATION_SASL_CONTINUE = 39,
    AUTHENTICATION_SASL_FINAL = 40,
    BACKEND_KEY_DATA = 41,
    GSSENC_REQUEST = 42,
    GSS_RESPONSE = 43,
    NEGOTIATE_PROTOCOL_VERSION = 44,
    PASSWORD_MESSAGE = 45,
    SASL_INITIAL_RESPONSE = 46,
    SASL_RESPONSE = 47,
    SSL_REQUEST = 48,
    STARTUP_MESSAGE = 49,

// simple query
    DATA_ROW = 100,
    EMPTY_QUERY_RESPONSE = 101,
    ROW_DESCRIPTION = 102,
    QUERY = 103,

// extended query
    BIND = 120,
    BIND_COMPLETE = 121,
    CLOSE = 122,
    CLOSE_COMPLETE = 123,
    DESCRIBE = 124,
    EXECUTE = 125,
    FLUSH = 126,
    NODATA = 127,
    PARAMETER_DESCRIPTION = 128,
    PARSE = 129,
    PARSE_COMPLETE = 130,
    PORTAL_SUSPENDED = 131,

// copy query
    COPY_DATA = 171,
    COPY_DONE = 172,
    COPY_FAIL = 173,
    COPY_IN_RESPONSE = 174,
    COPY_OUT_RESPONSE = 175,
    COPY_BOTH_RESPONSE = 176,

// function query (deprecated by the protocol)
    FUNCTION_CALL = 190,
    FUNCTION_CALL_RESPONSE = 191,
};

//// Column 'typelem' from 'pg_type' table. NB: not all types are compatible with PostgreSQL's ones
enum class ColumnType : Int32
{
    BOOL = 16,
    CHAR = 18,
    INT8 = 20,
    INT2 = 21,
    INT4 = 23,
    TEXT = 25,
    FLOAT4 = 700,
    FLOAT8 = 701,
    VARCHAR = 1043,
    DATE = 1082,
    TIMESTAMP = 1114,
    TIMESTAMPTZ = 1184,
    NUMERIC = 1700,
    UUID = 2950,
    JSONB = 3802,
};

class ColumnTypeSpec
{
public:
    ColumnType type;
    Int16 len;

    ColumnTypeSpec(ColumnType type_, Int16 len_) : type(type_), len(len_) {}
};

ColumnTypeSpec convertTypeIndexToPostgresColumnTypeSpec(TypeIndex type_index);

class MessageTransport
{
private:
    ReadBuffer * in;
    WriteBuffer * out;

public:
    MessageTransport(WriteBuffer * out_) : in(nullptr), out(out_) {}

    MessageTransport(ReadBuffer * in_, WriteBuffer * out_): in(in_), out(out_) {}

    template<typename TMessage>
    std::unique_ptr<TMessage> receiveWithPayloadSize(Int32 payload_size)
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>(payload_size);
        message->deserialize(*in);
        return message;
    }

    template<typename TMessage>
    std::unique_ptr<TMessage> receive()
    {
        std::unique_ptr<TMessage> message = std::make_unique<TMessage>();
        message->deserialize(*in);
        return message;
    }

    FrontMessageType receiveMessageType()
    {
        char type = 0;
        in->readStrict(type);
        return static_cast<FrontMessageType>(type);
    }

    template<typename TMessage>
    void send(TMessage & message, bool flush=false)
    {
        message.serialize(*out);
        if (flush)
            out->next();
    }

    template<typename TMessage>
    void send(TMessage && message, bool flush=false)
    {
        send(message, flush);
    }

    void send(char message, bool flush=false)
    {
        out->write(message);
        if (flush)
            out->next();
    }

    void dropMessage()
    {
        Int32 size;
        readBinaryBigEndian(size, *in);
        in->ignore(size - 4);
    }

    void flush()
    {
        out->next();
    }
};

/** Basic class for messages sent by client or server. */
class IMessage
{
public:
    virtual MessageType getMessageType() const = 0;

    virtual ~IMessage() = default;
};

class ISerializable
{
public:
    /** Should be overridden for sending the message */
    virtual void serialize(WriteBuffer & out) const = 0;

    /** Size of the message in bytes including message length part (4 bytes) */
    virtual Int32 size() const = 0;

    ISerializable() = default;

    ISerializable(const ISerializable &) = default;

    virtual ~ISerializable() = default;
};

class FrontMessage : public IMessage
{
public:
    /** Should be overridden for receiving the message
     * NB: This method should not read the first byte, which means the type of the message
     * (if type is provided for the message by the protocol).
     */
    virtual void deserialize(ReadBuffer & in) = 0;
};

class BackendMessage : public IMessage, public ISerializable
{};

class FirstMessage : public FrontMessage
{
public:
    Int32 payload_size;

    FirstMessage() = delete;
    FirstMessage(int payload_size_) : payload_size(payload_size_) {}
};

class CancelRequest : public FirstMessage
{
public:
    Int32 process_id = 0;
    Int32 secret_key = 0;

    CancelRequest(int payload_size_) : FirstMessage(payload_size_) {}

    void deserialize(ReadBuffer & in) override
    {
        readBinaryBigEndian(process_id, in);
        readBinaryBigEndian(secret_key, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::CANCEL_REQUEST;
    }
};

class ErrorOrNoticeResponse : BackendMessage
{
public:
    enum Severity {ERROR = 0, FATAL = 1, PANIC = 2, WARNING = 3, NOTICE = 4, DEBUG = 5, INFO = 6, LOG = 7};

private:
    Severity severity;
    String sql_state;
    String message;

    String enum_to_string[8] = {"ERROR", "FATAL", "PANIC", "WARNING", "NOTICE", "DEBUG", "INFO", "LOG"};

    char isErrorOrNotice() const
    {
        switch (severity)
        {
            case ERROR:
            case FATAL:
            case PANIC:
                return 'E';
            case WARNING:
            case NOTICE:
            case DEBUG:
            case INFO:
            case LOG:
                return 'N';
        }
        throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown severity type {}", std::to_string(severity));
    }

public:
    ErrorOrNoticeResponse(const Severity & severity_, const String & sql_state_, const String & message_)
    : severity(severity_)
    , sql_state(sql_state_)
    , message(message_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write(isErrorOrNotice());
        Int32 sz = size();
        writeBinaryBigEndian(sz, out);

        out.write('S');
        writeNullTerminatedString(enum_to_string[severity], out);
        out.write('C');
        writeNullTerminatedString(sql_state, out);
        out.write('M');
        writeNullTerminatedString(message, out);

        out.write(0);
    }

    Int32 size() const override
    {
        // message length part + (1 + sizes of other fields + 1) + null byte in the end of the message
        return static_cast<Int32>(
            4 +
            (1 + enum_to_string[severity].size() + 1) +
            (1 + sql_state.size() + 1) +
            (1 + message.size() + 1) +
            1);
    }

    MessageType getMessageType() const override
    {
        if (isErrorOrNotice() == 'E')
            return MessageType::ERROR_RESPONSE;
        return MessageType::NOTICE_RESPONSE;
    }
};

class ReadyForQuery : BackendMessage
{
public:
    void serialize(WriteBuffer &out) const override
    {
        out.write('Z');
        writeBinaryBigEndian(size(), out);
        // 'I' means that we are not in a transaction block. We use it here, because ClickHouse doesn't support transactions.
        out.write('I');
    }

    Int32 size() const override
    {
        return 4 + 1;
    }

    MessageType getMessageType() const override
    {
        return MessageType::READY_FOR_QUERY;
    }
};

class Terminate : FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        in.ignore(4);
    }

    MessageType getMessageType() const override
    {
        return MessageType::TERMINATE;
    }
};

class StartupMessage : FirstMessage
{
public:
    String user;
    String database;
    // includes username, may also include database and other runtime parameters
    std::unordered_map<String, String> parameters;

    StartupMessage(Int32 payload_size_) : FirstMessage(payload_size_) {}

    void deserialize(ReadBuffer & in) override
    {
        Int32 ps = payload_size - 1;
        while (ps > 0)
        {
            String parameter_name;
            String parameter_value;
            readNullTerminated(parameter_name, in);
            readNullTerminated(parameter_value, in);
            ps -= parameter_name.size() + 1;
            ps -= parameter_value.size() + 1;

            if (parameter_name == "user")
            {
                user = parameter_value;
            }
            else if (parameter_name == "database")
            {
                database = parameter_value;
            }

            parameters.insert({std::move(parameter_name), std::move(parameter_value)});

            if (payload_size < 0)
            {
                throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                                "Size of payload is larger than one declared in the message of type {}.",
                                static_cast<UInt64>(getMessageType()));
            }
        }
        in.ignore();
    }

    MessageType getMessageType() const override
    {
        return MessageType::STARTUP_MESSAGE;
    }
};

class AuthenticationCleartextPassword : public Messaging::BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int32>(3), out); // specifies that a clear-text password is required (by protocol)
    }

    Int32 size() const override
    {
        // length of message + special int32
        return 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_CLEARTEXT_PASSWORD;
    }
};

class AuthenticationOk : BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('R');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(0, out); // specifies that the authentication was successful (by protocol)
    }

    Int32 size() const override
    {
        // length of message + special int32
        return 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::AUTHENTICATION_OK;
    }
};

class PasswordMessage : FrontMessage
{
public:
    String password;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(password, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::PASSWORD_MESSAGE;
    }
};

class ParameterStatus : BackendMessage
{
private:
    String name;
    String value;

public:
    ParameterStatus(String name_, String value_)
    : name(name_)
    , value(value_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('S');
        writeBinaryBigEndian(size(), out);
        writeNullTerminatedString(name, out);
        writeNullTerminatedString(value, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(4 + name.size() + 1 + value.size() + 1);
    }

    MessageType getMessageType() const override
    {
        return MessageType::PARAMETER_STATUS;
    }
};

class BackendKeyData : BackendMessage
{
private:
    Int32 process_id;
    Int32 secret_key;

public:
    BackendKeyData(Int32 process_id_, Int32 secret_key_)
    : process_id(process_id_)
    , secret_key(secret_key_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('K');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(process_id, out);
        writeBinaryBigEndian(secret_key, out);
    }

    Int32 size() const override
    {
        return 4 + 4 + 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::BACKEND_KEY_DATA;
    }
};

class Query : FrontMessage
{
public:
    String query;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(query, in);
    }

    MessageType getMessageType() const override
    {
        return MessageType::QUERY;
    }
};

class EmptyQueryResponse : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('I');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override
    {
        return 4;
    }

    MessageType getMessageType() const override
    {
        return MessageType::EMPTY_QUERY_RESPONSE;
    }
};

enum class FormatCode : Int16
{
    TEXT = 0,
    BINARY = 1,
};

class FieldDescription : ISerializable
{
private:
    const String & name;
    ColumnTypeSpec type_spec;
    FormatCode format_code;

public:
    FieldDescription(const String & name_, TypeIndex type_index, FormatCode format_code_ = FormatCode::TEXT)
    : name(name_)
    , type_spec(convertTypeIndexToPostgresColumnTypeSpec(type_index))
    , format_code(format_code_)
    {}

    void serialize(WriteBuffer & out) const override
    {
        writeNullTerminatedString(name, out);
        writeBinaryBigEndian(static_cast<Int32>(0), out);
        writeBinaryBigEndian(static_cast<Int16>(0), out);
        writeBinaryBigEndian(static_cast<Int32>(type_spec.type), out);
        writeBinaryBigEndian(type_spec.len, out);
        writeBinaryBigEndian(static_cast<Int32>(-1), out);
        writeBinaryBigEndian(static_cast<Int16>(format_code), out);
    }

    Int32 size() const override
    {
        // size of name (C string)
        // + object ID of the table (Int32 and always zero) + attribute number of the column (Int16 and always zero)
        // + type object id (Int32) + data type size (Int16)
        // + type modifier (Int32 and always -1) + format code (Int16)
        return static_cast<Int32>((name.size() + 1) + 4 + 2 + 4 + 2 + 4 + 2);
    }
};

class RowDescription : BackendMessage
{
private:
    const std::vector<FieldDescription> & fields_descr;

public:
    RowDescription(const std::vector<FieldDescription> & fields_descr_) : fields_descr(fields_descr_) {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('T');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int16>(fields_descr.size()), out);
        for (const FieldDescription & field : fields_descr)
            field.serialize(out);
    }

    Int32 size() const override
    {
        Int32 sz = 4 + 2; // size of message + number of fields
        for (const FieldDescription & field : fields_descr)
            sz += field.size();
        return sz;
    }

    MessageType getMessageType() const override
    {
        return MessageType::ROW_DESCRIPTION;
    }
};

class StringField : public ISerializable
{
private:
    String str;
public:
    StringField(String str_) : str(str_) {}

    void serialize(WriteBuffer & out) const override
    {
        writeString(str, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(str.size());
    }
};

class NullField : public ISerializable
{
public:
    void serialize(WriteBuffer & /* out */) const override {}

    Int32 size() const override
    {
        return -1;
    }
};

class DataRow : BackendMessage
{
private:
    const std::vector<std::shared_ptr<ISerializable>> & row;

public:
    DataRow(const std::vector<std::shared_ptr<ISerializable>> & row_) : row(row_) {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('D');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int16>(row.size()), out);
        for (const std::shared_ptr<ISerializable> & field : row)
        {
            Int32 sz = field->size();
            writeBinaryBigEndian(sz, out);
            if (sz > 0)
                field->serialize(out);
        }
    }

    Int32 size() const override
    {
        Int32 sz = 4 + 2; // size of message + number of fields
        /// If values is NULL, field size is -1 and data not added.
        for (const std::shared_ptr<ISerializable> & field : row)
            sz += 4 + (field->size() > 0 ? field->size() : 0);
        return sz;
    }

    MessageType getMessageType() const override
    {
        return MessageType::DATA_ROW;
    }
};

class CommandComplete : BackendMessage
{
public:
    enum Command {BEGIN = 0, COMMIT = 1, INSERT = 2, DELETE = 3, UPDATE = 4, SELECT = 5, MOVE = 6, FETCH = 7, COPY = 8};
private:
    String enum_to_string[9] = {"BEGIN", "COMMIT", "INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY"};

    String value;

public:
    CommandComplete(Command cmd_, Int32 rows_count_)
    {
        value = enum_to_string[cmd_];
        String add = " ";
        if (cmd_ == Command::INSERT)
            add = " 0 ";
        value += add + std::to_string(rows_count_);
    }

    void serialize(WriteBuffer & out) const override
    {
        out.write('C');
        writeBinaryBigEndian(size(), out);
        writeNullTerminatedString(value, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(4 + value.size() + 1);
    }

    MessageType getMessageType() const override
    {
        return MessageType::COMMAND_COMPLETE;
    }

    static Command classifyQuery(const String & query)
    {
        std::vector<String> query_types({"BEGIN", "COMMIT", "INSERT", "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY"});
        for (size_t i = 0; i != query_types.size(); ++i)
        {
            String::const_iterator iter = std::search(
                query.begin(),
                query.end(),
                query_types[i].begin(),
                query_types[i].end(),
                [](char a, char b){return std::toupper(a) == b;});

            if (iter != query.end())
                return static_cast<Command>(i);
        }

        return Command::SELECT;
    }
};

// Extended query protocol front messages

class Parse : public FrontMessage
{
public:
    String statement_name;
    String query;
    std::vector<Int32> param_oids;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        Int32 remaining = sz - 4;

        readNullTerminated(statement_name, in);
        remaining -= static_cast<Int32>(statement_name.size() + 1);

        readNullTerminated(query, in);
        remaining -= static_cast<Int32>(query.size() + 1);

        Int16 num_params;
        readBinaryBigEndian(num_params, in);
        remaining -= 2;

        param_oids.resize(num_params);
        for (Int16 i = 0; i < num_params; ++i)
        {
            readBinaryBigEndian(param_oids[i], in);
            remaining -= 4;
        }

        if (remaining > 0)
            in.ignore(remaining);
    }

    MessageType getMessageType() const override { return MessageType::PARSE; }
};

class Bind : public FrontMessage
{
public:
    String portal_name;
    String statement_name;
    std::vector<Int16> param_format_codes;
    std::vector<std::optional<String>> param_values;
    std::vector<Int16> result_format_codes;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);

        readNullTerminated(portal_name, in);
        readNullTerminated(statement_name, in);

        Int16 num_format_codes;
        readBinaryBigEndian(num_format_codes, in);
        param_format_codes.resize(num_format_codes);
        for (Int16 i = 0; i < num_format_codes; ++i)
            readBinaryBigEndian(param_format_codes[i], in);

        Int16 num_params;
        readBinaryBigEndian(num_params, in);
        param_values.resize(num_params);
        for (Int16 i = 0; i < num_params; ++i)
        {
            Int32 len;
            readBinaryBigEndian(len, in);
            if (len == -1)
            {
                param_values[i] = std::nullopt;  // NULL parameter
            }
            else
            {
                String val;
                val.resize(len);
                in.readStrict(val.data(), len);
                param_values[i] = std::move(val);
            }
        }

        Int16 num_result_codes;
        readBinaryBigEndian(num_result_codes, in);
        result_format_codes.resize(num_result_codes);
        for (Int16 i = 0; i < num_result_codes; ++i)
            readBinaryBigEndian(result_format_codes[i], in);
    }

    MessageType getMessageType() const override { return MessageType::BIND; }
};

class Describe : public FrontMessage
{
public:
    char describe_type = 0;  // 'S' = statement, 'P' = portal
    String name;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        in.readStrict(describe_type);
        readNullTerminated(name, in);
    }

    MessageType getMessageType() const override { return MessageType::DESCRIBE; }
};

class Execute : public FrontMessage
{
public:
    String portal_name;
    Int32 max_rows = 0;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        readNullTerminated(portal_name, in);
        readBinaryBigEndian(max_rows, in);
    }

    MessageType getMessageType() const override { return MessageType::EXECUTE; }
};

class CloseMsg : public FrontMessage
{
public:
    char close_type = 0;  // 'S' or 'P'
    String name;

    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
        in.readStrict(close_type);
        readNullTerminated(name, in);
    }

    MessageType getMessageType() const override { return MessageType::CLOSE; }
};

class FlushMsg : public FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
    }

    MessageType getMessageType() const override { return MessageType::FLUSH; }
};

class SyncMsg : public FrontMessage
{
public:
    void deserialize(ReadBuffer & in) override
    {
        Int32 sz;
        readBinaryBigEndian(sz, in);
    }

    MessageType getMessageType() const override { return MessageType::SYNC; }
};

// Extended query protocol backend messages

class ParseComplete : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('1');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override { return 4; }
    MessageType getMessageType() const override { return MessageType::PARSE_COMPLETE; }
};

class BindComplete : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('2');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override { return 4; }
    MessageType getMessageType() const override { return MessageType::BIND_COMPLETE; }
};

class CloseCompleteMsg : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('3');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override { return 4; }
    MessageType getMessageType() const override { return MessageType::CLOSE_COMPLETE; }
};

class NoDataMsg : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('n');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override { return 4; }
    MessageType getMessageType() const override { return MessageType::NODATA; }
};

class ParameterDescription : public BackendMessage
{
private:
    std::vector<Int32> param_oids;

public:
    ParameterDescription() = default;
    explicit ParameterDescription(std::vector<Int32> oids) : param_oids(std::move(oids)) {}

    void serialize(WriteBuffer & out) const override
    {
        out.write('t');
        writeBinaryBigEndian(size(), out);
        writeBinaryBigEndian(static_cast<Int16>(param_oids.size()), out);
        for (const auto & oid : param_oids)
            writeBinaryBigEndian(oid, out);
    }

    Int32 size() const override
    {
        return static_cast<Int32>(4 + 2 + param_oids.size() * 4);
    }

    MessageType getMessageType() const override { return MessageType::PARAMETER_DESCRIPTION; }
};

class PortalSuspended : public BackendMessage
{
public:
    void serialize(WriteBuffer & out) const override
    {
        out.write('s');
        writeBinaryBigEndian(size(), out);
    }

    Int32 size() const override { return 4; }
    MessageType getMessageType() const override { return MessageType::PORTAL_SUSPENDED; }
};

}

namespace PGAuthentication
{

class AuthenticationMethod
{
protected:
    static void setPassword(
        const String & user_name,
        const String & password,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address)
    {
        try
        {
            session.authenticate(user_name, password, address);
        }
        catch (const Exception &)
        {
            mt.send(
                Messaging::ErrorOrNoticeResponse(Messaging::ErrorOrNoticeResponse::ERROR, "28P01", "Invalid user or password"),
                true);
            throw;
        }
    }

public:
    virtual void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) = 0;

    virtual AuthenticationType getType() const = 0;

    virtual ~AuthenticationMethod() = default;
};

class NoPasswordAuth : public AuthenticationMethod
{
public:
    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) override
    {
        return setPassword(user_name, "", session, mt, address);
    }

    AuthenticationType getType() const override
    {
        return AuthenticationType::NO_PASSWORD;
    }
};

class CleartextPasswordAuth : public AuthenticationMethod
{
public:
    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address) override
    {
        mt.send(Messaging::AuthenticationCleartextPassword(), true);

        Messaging::FrontMessageType type = mt.receiveMessageType();
        if (type == Messaging::FrontMessageType::PASSWORD_MESSAGE)
        {
            std::unique_ptr<Messaging::PasswordMessage> password = mt.receive<Messaging::PasswordMessage>();
            return setPassword(user_name, password->password, session, mt, address);
        }
        else
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                    "Client sent wrong message or closed the connection. Message byte was {}.",
                    static_cast<Int32>(type));
    }

    AuthenticationType getType() const override
    {
        return AuthenticationType::PLAINTEXT_PASSWORD;
    }
};

class AuthenticationManager
{
private:
    LoggerPtr log = getLogger("AuthenticationManager");
    std::unordered_map<AuthenticationType, std::shared_ptr<AuthenticationMethod>> type_to_method = {};

public:
    AuthenticationManager(const std::vector<std::shared_ptr<AuthenticationMethod>> & auth_methods)
    {
        for (const std::shared_ptr<AuthenticationMethod> & method : auth_methods)
        {
            type_to_method[method->getType()] = method;
        }
    }

    void authenticate(
        const String & user_name,
        Session & session,
        Messaging::MessageTransport & mt,
        const Poco::Net::SocketAddress & address)
    {
        const AuthenticationType user_auth_type = session.getAuthenticationTypeOrLogInFailure(user_name);
        if (type_to_method.find(user_auth_type) != type_to_method.end())
        {
            type_to_method[user_auth_type]->authenticate(user_name, session, mt, address);
            mt.send(Messaging::AuthenticationOk(), true);
            LOG_DEBUG(log, "Authentication for user {} was successful.", user_name);
            return;
        }

        mt.send(
            Messaging::ErrorOrNoticeResponse(Messaging::ErrorOrNoticeResponse::ERROR, "0A000", "Authentication method is not supported"),
            true);

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Authentication type {} is not supported.", user_auth_type);
    }
};
}

}
}
