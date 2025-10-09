#pragma once

#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConnectionTimeouts.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Common/sendRequest.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Timespan.h>

namespace DB
{

namespace ErrorCodes
{
extern const int REMOTE_CALL_FAILED;
extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

class RemoteUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    explicit RemoteUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_)
        : UserDefinedFunctionBase(std::move(udf_desc_), std::move(context_), "RemoteUserDefinedFunction")
    {
    }

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        Block block(arguments);

        auto remote_udf_payload = std::dynamic_pointer_cast<cluster::protocol::RemoteUserDefinedFunctionPayload>(udf_desc->payload);

        /// convert block to 'JSONColumns' string
        String out;
        WriteBufferFromString out_buf{out};
        auto convert = [&](const Block & block_to_send) {
            auto out_format = context->getOutputFormat("JSONColumns", out_buf, block_to_send.cloneEmpty());
            out_format->setAutoFlush();
            out_format->write(block_to_send);
            out_format->finalize();
            out_buf.finalize();
        };
        convert(block);

        /// send data to remote endpoint
        auto uri = Poco::URI(remote_udf_payload->url);
        Poco::Timespan recv_timeout{static_cast<Poco::Timespan::TimeDiff>(remote_udf_payload->command_read_timeout_milliseconds * 1000)};
        auto timeouts = ConnectionTimeouts().withConnectionTimeout(2).withSendTimeout(5).withReceiveTimeout(recv_timeout);
        auto [resp, http_status] = sendRequest(
            uri,
            Poco::Net::HTTPRequest::HTTP_POST,
            context->getCurrentQueryId(),
            "",
            "",
            out,
            {{remote_udf_payload->auth_context.key_name, remote_udf_payload->auth_context.key_value}, {"", context->getCurrentQueryId()}},
            timeouts,
            logger);

        if (http_status != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(
                ErrorCodes::REMOTE_CALL_FAILED,
                "Call remote uri {} failed, message: {}, http_status: {}",
                uri.toString(),
                resp,
                http_status);

        /// Convert resp to block
        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});
        ReadBufferFromString read_buffer(resp);
        auto input_format = context->getInputFormat("JSONColumns", read_buffer, result_header, input_rows_count);
        try
        {
            return pull(input_format, result_type, input_rows_count, remote_udf_payload->command_read_timeout_milliseconds);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
                e.addMessage("The result format is invalid, which should be 'JSONColumns'");
            throw;
        }
    }

    bool isSuitableForConstantFolding() const override { return false; }
};

}
