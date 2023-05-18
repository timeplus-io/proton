#pragma once

#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <Processors/Sources/ShellCommandSource.h>

namespace DB
{

class ExecutableUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    explicit ExecutableUserDefinedFunction(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_)
        : UserDefinedFunctionBase(std::move(executable_function_), std::move(context_), "ExecutableUserDefinedFunction")
    {
        const auto & config = executable_function->getConfiguration()->as<const ExecutableUserDefinedFunctionConfiguration &>();
        ShellCommandSourceCoordinator::Configuration shell_command_coordinator_configration{
            .format = config.format,
            .command_termination_timeout_seconds = config.command_termination_timeout_seconds,
            .command_read_timeout_milliseconds = config.command_read_timeout_milliseconds,
            .command_write_timeout_milliseconds = config.command_write_timeout_milliseconds,
            .pool_size = config.pool_size,
            .max_command_execution_time_seconds = config.max_command_execution_time_seconds,
            .is_executable_pool = config.is_executable_pool,
            .send_chunk_header = config.send_chunk_header,
            .execute_direct = config.execute_direct};
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
        const auto & config = executable_function->getConfiguration()->as<const ExecutableUserDefinedFunctionConfiguration &>();

        ColumnWithTypeAndName result(result_type, "result");
        Block result_header({result});

        auto ctx = coordinator->getUDFContext(config.command, config.command_arguments, block.cloneEmpty(), result_header, context);

        coordinator->sendData(ctx, block);

        auto result_column = coordinator->pull(ctx, result_type, input_rows_count);
        LOG_TRACE(log, "pull {} rows", result_column->size());
        coordinator->release(std::move(ctx));

        return result_column;
    }

private:
    std::shared_ptr<ShellCommandSourceCoordinator> coordinator = nullptr;
};

}
