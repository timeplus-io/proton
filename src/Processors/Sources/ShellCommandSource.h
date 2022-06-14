#pragma once

#include <memory>

#include <base/logger_useful.h>
#include <base/BorrowedObjectPool.h>

#include <Common/ShellCommand.h>
#include <Common/ThreadPool.h>

#include <IO/ReadHelpers.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Formats/IInputFormat.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

/// proton: starts
#include <NativeLog/Base/Concurrent/BlockingQueue.h>
/// proton: ends


namespace DB
{

class ShellCommandHolder;
using ShellCommandHolderPtr = std::unique_ptr<ShellCommandHolder>;

using ProcessPool = BorrowedObjectPool<ShellCommandHolderPtr>;

class UDFExecutionContext;
using UDFExecutionContextPtr = std::unique_ptr<UDFExecutionContext>;
using UDFExecutionContextPool = BorrowedObjectPool<UDFExecutionContextPtr>;

struct ShellCommandSourceConfiguration
{
    /// Read fixed number of rows from command output
    bool read_fixed_number_of_rows = false;
    /// Valid only if read_fixed_number_of_rows = true
    bool read_number_of_rows_from_process_output = false;
    /// Valid only if read_fixed_number_of_rows = true
    size_t number_of_rows_to_read = 0;
    /// Max block size
    size_t max_block_size = DEFAULT_BLOCK_SIZE;
};

class TimeoutReadBufferFromFileDescriptor;
class TimeoutWriteBufferFromFileDescriptor;

/// The execution context used in UDF execution. It stores the running status of UDF, including
/// - subprocess of UDF
/// - input/output format used to convert and transfer input block and result
/// - multi-thread queue
/// - thread used to send data
class UDFExecutionContext
{
public:
    explicit UDFExecutionContext(
        std::unique_ptr<ShellCommand> process_,
        OutputFormatPtr out_,
        InputFormatPtr in_,
        std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> write_buffer_,
        std::shared_ptr<TimeoutReadBufferFromFileDescriptor> read_buffer_)
        : process(std::move(process_))
        , out_format(std::move(out_))
        , in_format(std::move(in_))
        , write_buffer(std::move(write_buffer_))
        , read_buffer(std::move(read_buffer_))
    {
    }

public:
    std::unique_ptr<ShellCommand> process;
    nlog::BlockingQueue<Block> input_block_queue{1024};
    OutputFormatPtr out_format;
    InputFormatPtr in_format;
    std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> write_buffer;
    std::shared_ptr<TimeoutReadBufferFromFileDescriptor> read_buffer;

    /// For send_data_thread
    ThreadFromGlobalPool send_data_thread;
    std::atomic<bool> is_shutdown{false};

    /// status tracking
    std::atomic<bool> command_is_invalid{false};

    ~UDFExecutionContext();
};

class ShellCommandSourceCoordinator
{
public:

    struct Configuration
    {

        /// Script output format
        std::string format;

        /// Command termination timeout in seconds
        size_t command_termination_timeout_seconds = 10;

        /// Timeout for reading data from command stdout
        size_t command_read_timeout_milliseconds = 10000;

        /// Timeout for writing data to command stdin
        size_t command_write_timeout_milliseconds = 10000;

        /// Pool size valid only if executable_pool = true
        size_t pool_size = 16;

        /// Max command execution time in milliseconds. Valid only if executable_pool = true
        size_t max_command_execution_time_seconds = 10;

        /// Should pool of processes be created.
        bool is_executable_pool = false;

        /// Send number_of_rows\n before sending chunk to process.
        bool send_chunk_header = false;

        /// Execute script direct or with /bin/bash.
        bool execute_direct = true;

    };

    explicit ShellCommandSourceCoordinator(const Configuration & configuration_);

    const Configuration & getConfiguration() const
    {
        return configuration;
    }

    Pipe createPipe(
        const std::string & command,
        const std::vector<std::string> & arguments,
        std::vector<Pipe> && input_pipes,
        Block sample_block,
        ContextPtr context,
        const ShellCommandSourceConfiguration & source_configuration = {});

    Pipe createPipe(
        const std::string & command,
        std::vector<Pipe> && input_pipes,
        Block sample_block,
        ContextPtr context,
        const ShellCommandSourceConfiguration & source_configuration = {})
    {
        return createPipe(command, {}, std::move(input_pipes), std::move(sample_block), std::move(context), source_configuration);
    }

    Pipe createPipe(
        const std::string & command,
        const std::vector<std::string> & arguments,
        Block sample_block,
        ContextPtr context)
    {
        return createPipe(command, arguments, {}, std::move(sample_block), std::move(context), {});
    }

    Pipe createPipe(
        const std::string & command,
        Block sample_block,
        ContextPtr context)
    {
        return createPipe(command, {}, {}, std::move(sample_block), std::move(context), {});
    }

    /// proton: starts
    UDFExecutionContextPtr getUDFContext(
        const std::string & command,
        const std::vector<std::string> & arguments,
        Block input_header,
        Block result_header,
        ContextPtr context);
    void release(UDFExecutionContextPtr && ctx);
    static void sendData(UDFExecutionContextPtr & ctx, const Block & block);
    ColumnPtr pull(UDFExecutionContextPtr & ctx, const DataTypePtr & result_type, size_t result_rows_count);
    ColumnPtr pull(InputFormatPtr & in_format, const DataTypePtr & result_type, size_t result_rows_count);

    void stopProcessPool();
    /// proton: ends

private:

    Configuration configuration;

    std::shared_ptr<ProcessPool> process_pool = nullptr;
    std::shared_ptr<UDFExecutionContextPool> udf_ctx_pool = nullptr;
};

}
