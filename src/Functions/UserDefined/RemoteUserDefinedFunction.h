#pragma once

#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ConnectionTimeouts.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Common/sendRequest.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

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
    explicit RemoteUserDefinedFunction(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_)
        : UserDefinedFunctionBase(std::move(executable_function_), std::move(context_), "RemoteUserDefinedFunction")
    {
    }

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        Block block(arguments);
        const auto & config = executable_function->getConfiguration()->as<const RemoteUserDefinedFunctionConfiguration &>();
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
        auto [resp, http_status] = sendRequest(
            config.url,
            Poco::Net::HTTPRequest::HTTP_POST,
            context->getCurrentQueryId(),
            "",
            "",
            out,
            {{config.auth_context.key_name, config.auth_context.key_value}, {"", context->getCurrentQueryId()}},
            ConnectionTimeouts({2, 0}/* connect timeout */, {5, 0} /* send timeout */, {static_cast<long>(config.command_execution_timeout_milliseconds / 1000), static_cast<long>((config.command_execution_timeout_milliseconds % 1000u) * 1000u)/* receive timeout */}), ///  timeout and limit for connect/send/receive ...
            &Poco::Logger::get("UserDefinedFunction"));

        if (http_status != Poco::Net::HTTPResponse::HTTP_OK)
            throw Exception(
                ErrorCodes::REMOTE_CALL_FAILED,
                "Call remote uri {} failed, message: {}, http_status: {}",
                config.url.toString(),
                resp,
                http_status);

        /// Convert resp to block
        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});
        ReadBufferFromString read_buffer(resp);
        auto input_format = context->getInputFormat("JSONColumns", read_buffer, result_header, input_rows_count);
        try
        {
            return pull(input_format, result_type, input_rows_count, config.command_read_timeout_milliseconds);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
                e.addMessage("The result format is invalid, which should be 'JSONColumns'");
            throw;
        }
    }
};

}
