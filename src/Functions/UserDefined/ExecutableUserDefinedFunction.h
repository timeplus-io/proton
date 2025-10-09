#pragma once

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <Processors/Sources/ShellCommandSource.h>

namespace DB
{

class ExecutableUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    explicit ExecutableUserDefinedFunction(cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_, ContextPtr context_)
        : UserDefinedFunctionBase(std::move(udf_desc_), std::move(context_), "ExecutableUserDefinedFunction")
    {
        auto executable_udf_payload = std::dynamic_pointer_cast<cluster::protocol::ExecutableUserDefinedFunctionPayload>(udf_desc->payload);

        ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration{
            .format = executable_udf_payload->format,
            .command_termination_timeout_seconds = executable_udf_payload->command_termination_timeout_seconds,
            .command_read_timeout_milliseconds = executable_udf_payload->command_read_timeout_milliseconds,
            .command_write_timeout_milliseconds = executable_udf_payload->command_write_timeout_milliseconds,
            .pool_size = executable_udf_payload->pool_size,
            .max_command_execution_time_seconds = executable_udf_payload->max_command_execution_time_seconds,
            .is_executable_pool = executable_udf_payload->is_executable_pool,
            .send_chunk_header = executable_udf_payload->send_chunk_header,
            .execute_direct = executable_udf_payload->execute_direct};
        coordinator = std::make_shared<ShellCommandSourceCoordinator>(shell_command_coordinator_configration);
    }

    ~ExecutableUserDefinedFunction() override
    {
        /// query ends, stop all udf processes in process pool
        if (coordinator)
            coordinator->stopProcessPool();
    }

    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        Block block(arguments);

        auto executable_udf_payload = std::dynamic_pointer_cast<cluster::protocol::ExecutableUserDefinedFunctionPayload>(udf_desc->payload);

        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});

        auto ctx = coordinator->getUDFContext(
            executable_udf_payload->command, executable_udf_payload->command_arguments, block.cloneEmpty(), result_header, context);

        coordinator->sendData(ctx, block);

        auto result_column = coordinator->pull(ctx, result_type, input_rows_count);
        LOG_TRACE(logger, "pull {} rows", result_column->size());
        coordinator->release(std::move(ctx));

        return result_column;
    }

private:
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator = nullptr;
};

}
