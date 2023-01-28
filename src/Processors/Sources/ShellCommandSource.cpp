#include <Processors/Sources/ShellCommandSource.h>

#include <sys/poll.h>

#include <Common/Stopwatch.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Interpreters/Context.h>

/// proton: starts
#include <Interpreters/UserDefinedFunctionConfiguration.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
/// proton: ends


namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_POLL;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

static bool tryMakeFdNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;
    if (-1 == fcntl(fd, F_SETFL, flags | O_NONBLOCK))
        return false;

    return true;
}

static void makeFdNonBlocking(int fd)
{
    bool result = tryMakeFdNonBlocking(fd);
    if (!result)
        throwFromErrno("Cannot set non-blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

static bool tryMakeFdBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (-1 == flags)
        return false;

    if (-1 == fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)))
        return false;

    return true;
}

static void makeFdBlocking(int fd)
{
    bool result = tryMakeFdBlocking(fd);
    if (!result)
        throwFromErrno("Cannot set blocking mode of pipe", ErrorCodes::CANNOT_FCNTL);
}

static bool pollFd(int fd, size_t timeout_milliseconds, int events)
{
    pollfd pfd;
    pfd.fd = fd;
    pfd.events = events;
    pfd.revents = 0;

    Stopwatch watch;

    int res;

    while (true)
    {
        res = poll(&pfd, 1, static_cast<int>(timeout_milliseconds));

        if (res < 0)
        {
            if (errno == EINTR)
            {
                watch.stop();
                timeout_milliseconds -= watch.elapsedMilliseconds();
                watch.start();

                continue;
            }
            else
            {
                throwFromErrno("Cannot poll", ErrorCodes::CANNOT_POLL);
            }
        }
        else
        {
            break;
        }
    }

    return res > 0;
}

class TimeoutReadBufferFromFileDescriptor : public BufferWithOwnMemoryNoTrack<ReadBuffer>
{
public:
    explicit TimeoutReadBufferFromFileDescriptor(int fd_, size_t timeout_milliseconds_)
        : fd(fd_)
        , timeout_milliseconds(timeout_milliseconds_)
    {
        makeFdNonBlocking(fd);
    }

    bool nextImpl() override
    {
        size_t bytes_read = 0;

        while (!bytes_read)
        {
            if (!pollFd(fd, timeout_milliseconds, POLLIN))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe read timeout exceeded {} milliseconds", timeout_milliseconds);

            ssize_t res = ::read(fd, internal_buffer.begin(), internal_buffer.size());

            if (-1 == res && errno != EINTR)
                throwFromErrno("Cannot read from pipe", ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

            if (res == 0)
                break;

            if (res > 0)
                bytes_read += res;
        }

        if (bytes_read > 0)
        {
            working_buffer = internal_buffer;
            working_buffer.resize(bytes_read);
        }
        else
        {
            return false;
        }

        return true;
    }

    void reset() const
    {
        makeFdBlocking(fd);
    }

    ~TimeoutReadBufferFromFileDescriptor() override
    {
        tryMakeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
};

class TimeoutWriteBufferFromFileDescriptor : public BufferWithOwnMemoryNoTrack<WriteBuffer>
{
public:
    explicit TimeoutWriteBufferFromFileDescriptor(int fd_, size_t timeout_milliseconds_)
        : fd(fd_)
        , timeout_milliseconds(timeout_milliseconds_)
    {
        makeFdNonBlocking(fd);
    }

    void nextImpl() override
    {
        if (!offset())
            return;

        size_t bytes_written = 0;

        while (bytes_written != offset())
        {
            if (!pollFd(fd, timeout_milliseconds, POLLOUT))
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Pipe write timeout exceeded {} milliseconds", timeout_milliseconds);

            ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

            if ((-1 == res || 0 == res) && errno != EINTR)
                throwFromErrno("Cannot write into pipe", ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

            if (res > 0)
                bytes_written += res;
        }
    }

    void reset() const
    {
        makeFdBlocking(fd);
    }

    ~TimeoutWriteBufferFromFileDescriptor() override
    {
        tryMakeFdBlocking(fd);
    }

private:
    int fd;
    size_t timeout_milliseconds;
};

class ShellCommandHolder
{
public:
    using ShellCommandBuilderFunc = std::function<std::unique_ptr<ShellCommand>()>;

    explicit ShellCommandHolder(ShellCommandBuilderFunc && func_)
        : func(std::move(func_))
    {}

    std::unique_ptr<ShellCommand> buildCommand()
    {
        if (returned_command)
            return std::move(returned_command);

        return func();
    }

    void returnCommand(std::unique_ptr<ShellCommand> command)
    {
        returned_command = std::move(command);
    }

private:
    std::unique_ptr<ShellCommand> returned_command;
    ShellCommandBuilderFunc func;
};

namespace
{
    /** A stream, that get child process and sends data using tasks in background threads.
    * For each send data task background thread is created. Send data task must send data to process input pipes.
    * ShellCommandPoolSource receives data from process stdout.
    *
    * If process_pool is passed in constructor then after source is destroyed process is returned to pool.
    */
    class ShellCommandSource final : public ISource
    {
    public:

        using SendDataTask = std::function<void(void)>;

        ShellCommandSource(
            ContextPtr context_,
            const std::string & format_,
            size_t command_read_timeout_milliseconds,
            const Block & sample_block_,
            std::unique_ptr<ShellCommand> && command_,
            std::vector<SendDataTask> && send_data_tasks = {},
            const ShellCommandSourceConfiguration & configuration_ = {},
            std::unique_ptr<ShellCommandHolder> && command_holder_ = nullptr,
            std::shared_ptr<ProcessPool> process_pool_ = nullptr)
            : ISource(sample_block_, true, ProcessorID::ShellCommandSourceID)
            , context(context_)
            , format(format_)
            , sample_block(sample_block_)
            , command(std::move(command_))
            , configuration(configuration_)
            , timeout_command_out(command->out.getFD(), command_read_timeout_milliseconds)
            , command_holder(std::move(command_holder_))
            , process_pool(process_pool_)
        {
            for (auto && send_data_task : send_data_tasks)
            {
                send_data_threads.emplace_back([task = std::move(send_data_task), this]()
                {
                    try
                    {
                        task();
                    }
                    catch (...)
                    {
                        std::lock_guard<std::mutex> lock(send_data_lock);
                        exception_during_send_data = std::current_exception();
                    }
                });
            }

            size_t max_block_size = configuration.max_block_size;

            if (configuration.read_fixed_number_of_rows)
            {
                /** Currently parallel parsing input format cannot read exactly max_block_size rows from input,
                  * so it will be blocked on ReadBufferFromFileDescriptor because this file descriptor represent pipe that does not have eof.
                  */
                auto context_for_reading = Context::createCopy(context);
                context_for_reading->setSetting("input_format_parallel_parsing", false);
                context = context_for_reading;

                if (configuration.read_number_of_rows_from_process_output)
                {
                    /// Initialize executor in generate
                    return;
                }

                max_block_size = configuration.number_of_rows_to_read;
            }

            pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, sample_block, max_block_size)));
            executor = std::make_unique<PullingPipelineExecutor>(pipeline);
        }

        ~ShellCommandSource() override
        {
            for (auto & thread : send_data_threads)
                if (thread.joinable())
                    thread.join();

            if (command_is_invalid)
                command = nullptr;

            if (command_holder && process_pool)
            {
                bool valid_command = configuration.read_fixed_number_of_rows && current_read_rows >= configuration.number_of_rows_to_read;

                if (command && valid_command)
                    command_holder->returnCommand(std::move(command));

                process_pool->returnObject(std::move(command_holder));
            }
        }

    protected:

        Chunk generate() override
        {
            rethrowExceptionDuringSendDataIfNeeded();

            Chunk chunk;

            try
            {
                if (configuration.read_fixed_number_of_rows)
                {
                    if (!executor && configuration.read_number_of_rows_from_process_output)
                    {
                        readText(configuration.number_of_rows_to_read, timeout_command_out);
                        char dummy;
                        readChar(dummy, timeout_command_out);

                        size_t max_block_size = configuration.number_of_rows_to_read;
                        pipeline = QueryPipeline(Pipe(context->getInputFormat(format, timeout_command_out, sample_block, max_block_size)));
                        executor = std::make_unique<PullingPipelineExecutor>(pipeline);
                    }

                    if (current_read_rows >= configuration.number_of_rows_to_read)
                        return {};
                }

                if (!executor->pull(chunk))
                    return {};

                current_read_rows += chunk.getNumRows();
            }
            catch (...)
            {
                command_is_invalid = true;
                throw;
            }

            return chunk;
        }

        Status prepare() override
        {
            auto status = ISource::prepare();

            if (status == Status::Finished)
            {
                for (auto & thread : send_data_threads)
                    if (thread.joinable())
                        thread.join();

                rethrowExceptionDuringSendDataIfNeeded();
            }

            return status;
        }

        String getName() const override { return "ShellCommandSource"; }

    private:

        void rethrowExceptionDuringSendDataIfNeeded()
        {
            std::lock_guard<std::mutex> lock(send_data_lock);
            if (exception_during_send_data)
            {
                command_is_invalid = true;
                std::rethrow_exception(exception_during_send_data);
            }
        }

        ContextPtr context;
        std::string format;
        Block sample_block;

        std::unique_ptr<ShellCommand> command;
        ShellCommandSourceConfiguration configuration;

        TimeoutReadBufferFromFileDescriptor timeout_command_out;

        size_t current_read_rows = 0;

        ShellCommandHolderPtr command_holder;
        std::shared_ptr<ProcessPool> process_pool;

        QueryPipeline pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;

        std::vector<ThreadFromGlobalPool> send_data_threads;

        std::mutex send_data_lock;
        std::exception_ptr exception_during_send_data;

        std::atomic<bool> command_is_invalid {false};
    };

    class SendingChunkHeaderTransform final : public ISimpleTransform
    {
    public:
        SendingChunkHeaderTransform(const Block & header, std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> buffer_)
            : ISimpleTransform(header, header, false, ProcessorID::SendingChunkHeaderTransformID)
            , buffer(buffer_)
        {
        }

        String getName() const override { return "SendingChunkHeaderTransform"; }

    protected:

        void transform(Chunk & chunk) override
        {
            writeText(chunk.getNumRows(), *buffer);
            writeChar('\n', *buffer);
        }

    private:
        std::shared_ptr<TimeoutWriteBufferFromFileDescriptor> buffer;
    };

}

ShellCommandSourceCoordinator::ShellCommandSourceCoordinator(const Configuration & configuration_)
    : configuration(configuration_)
{
    /// proton: starts.
    constexpr size_t max_pool_size = 100;
    process_pool = std::make_shared<ProcessPool>(configuration.pool_size ? configuration.pool_size : max_pool_size);
    udf_ctx_pool = std::make_shared<UDFExecutionContextPool>(configuration.pool_size ? configuration.pool_size : max_pool_size);
    js_ctx_pool = std::make_shared<JavaScriptExecutionContextPool>(configuration.pool_size ? configuration.pool_size : max_pool_size);
    /// proton: ends.
}

Pipe ShellCommandSourceCoordinator::createPipe(
    const std::string & command,
    const std::vector<std::string> & arguments,
    std::vector<Pipe> && input_pipes,
    Block sample_block,
    ContextPtr context,
    const ShellCommandSourceConfiguration & source_configuration)
{
    ShellCommand::Config command_config(command);
    command_config.arguments = arguments;
    for (size_t i = 1; i < input_pipes.size(); ++i)
        command_config.write_fds.emplace_back(i + 2);

    std::unique_ptr<ShellCommand> process;
    std::unique_ptr<ShellCommandHolder> process_holder;

    auto destructor_strategy = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, configuration.command_termination_timeout_seconds};
    command_config.terminate_in_destructor_strategy = destructor_strategy;

    bool is_executable_pool = (process_pool != nullptr);
    if (is_executable_pool)
    {
        bool execute_direct = configuration.execute_direct;

        bool result = process_pool->tryBorrowObject(
            process_holder,
            [command_config, execute_direct]()
            {
                ShellCommandHolder::ShellCommandBuilderFunc func = [command_config, execute_direct]() mutable
                {
                    if (execute_direct)
                        return ShellCommand::executeDirect(command_config);
                    else
                        return ShellCommand::execute(command_config);
                };

                return std::make_unique<ShellCommandHolder>(std::move(func));
            },
            configuration.max_command_execution_time_seconds * 1000);

        if (!result)
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED,
                "Could not get process from pool, max command execution timeout exceeded {} seconds",
                configuration.max_command_execution_time_seconds);

        process = process_holder->buildCommand();
    }
    else
    {
        if (configuration.execute_direct)
            process = ShellCommand::executeDirect(command_config);
        else
            process = ShellCommand::execute(command_config);
    }

    std::vector<ShellCommandSource::SendDataTask> tasks;
    tasks.reserve(input_pipes.size());

    for (size_t i = 0; i < input_pipes.size(); ++i)
    {
        WriteBufferFromFile * write_buffer = nullptr;

        if (i == 0)
        {
            write_buffer = &process->in;
        }
        else
        {
            int descriptor = static_cast<int>(i) + 2;
            auto it = process->write_fds.find(descriptor);
            if (it == process->write_fds.end())
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Process does not contain descriptor to write {}", descriptor);

            write_buffer = &it->second;
        }

        int write_buffer_fd = write_buffer->getFD();
        auto timeout_write_buffer = std::make_shared<TimeoutWriteBufferFromFileDescriptor>(write_buffer_fd, configuration.command_write_timeout_milliseconds);

        input_pipes[i].resize(1);

        if (configuration.send_chunk_header)
        {
            auto transform = std::make_shared<SendingChunkHeaderTransform>(input_pipes[i].getHeader(), timeout_write_buffer);
            input_pipes[i].addTransform(std::move(transform));
        }

        auto pipeline = std::make_shared<QueryPipeline>(std::move(input_pipes[i]));
        auto out = context->getOutputFormat(configuration.format, *timeout_write_buffer, materializeBlock(pipeline->getHeader()));
        out->setAutoFlush();
        pipeline->complete(std::move(out));

        ShellCommandSource::SendDataTask task = [pipeline, timeout_write_buffer, write_buffer, is_executable_pool]()
        {
            CompletedPipelineExecutor executor(*pipeline);
            executor.execute();

            if (!is_executable_pool)
            {
                timeout_write_buffer->next();
                timeout_write_buffer->reset();

                write_buffer->close();
            }
        };

        tasks.emplace_back(std::move(task));
    }

    auto source = std::make_unique<ShellCommandSource>(
        context,
        configuration.format,
        configuration.command_read_timeout_milliseconds,
        std::move(sample_block),
        std::move(process),
        std::move(tasks),
        source_configuration,
        std::move(process_holder),
        process_pool);

    return Pipe(std::move(source));
}

/// proton: starts
UDFExecutionContext::~UDFExecutionContext()
{
    /// stop send_data_thread
    bool old_val = false;
    if (send_data_thread.joinable() && is_shutdown.compare_exchange_strong(old_val, true))
        send_data_thread.join();

    /// stop subprocess
    process = nullptr;
}

JavaScriptExecutionContext::JavaScriptExecutionContext(const UserDefinedFunctionConfiguration & config, ContextPtr ctx)
{
    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    isolate = std::unique_ptr<v8::Isolate, IsolateDeleter>(v8::Isolate::New(isolate_params), IsolateDeleter());

    /// check heap limit
    V8::checkHeapLimit(isolate.get(), ctx->getSettingsRef().javascript_max_memory_bytes);

    auto init_functions = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> &) {
        v8::Local<v8::Value> function_val;
        if (!ctx->Global()->Get(ctx, V8::to_v8(isolate_, config.name)).ToLocal(&function_val) || !function_val->IsFunction())
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", config.name);

        func.Reset(isolate_, function_val.As<v8::Function>());

        context.Reset(isolate_, ctx);
    };

    V8::compileSource(isolate.get(), config.name, config.source, init_functions);
}

UDFExecutionContextPtr ShellCommandSourceCoordinator::getUDFContext(
    const std::string & command, const std::vector<std::string> & arguments, Block input_header, Block result_header, ContextPtr context)
{
    /// prepare sub-process of UDF
    ShellCommand::Config command_config(command);
    command_config.arguments = arguments;
    command_config.write_fds.emplace_back(3);

    auto destructor_strategy
        = ShellCommand::DestructorStrategy{true /*terminate_in_destructor*/, configuration.command_termination_timeout_seconds};
    command_config.terminate_in_destructor_strategy = destructor_strategy;

    assert(udf_ctx_pool);

    bool execute_direct = configuration.execute_direct;

    auto create_context = [this, context, command_config, execute_direct, input_header, result_header]() {
        /// start subprocess
        std::unique_ptr<ShellCommand> process;
        if (execute_direct)
            process = ShellCommand::executeDirect(command_config);
        else
            process = ShellCommand::execute(command_config);

        /// prepare out_format
        auto timeout_write_buffer = std::make_shared<TimeoutWriteBufferFromFileDescriptor>(
            process->in.getFD(), this->configuration.command_write_timeout_milliseconds);
        auto out_format = context->getOutputFormat(this->configuration.format, *timeout_write_buffer, input_header);

        /// prepare in_format
        auto timeout_read_buffer = std::make_shared<TimeoutReadBufferFromFileDescriptor>(
            process->out.getFD(), this->configuration.command_read_timeout_milliseconds);
        auto context_for_reading = Context::createCopy(context);
        context_for_reading->setSetting("input_format_parallel_parsing", false);
        auto in_format
            = context_for_reading->getInputFormat(this->configuration.format, *timeout_read_buffer, result_header, DEFAULT_BLOCK_SIZE);

        auto ctx = std::make_unique<UDFExecutionContext>(
            std::move(process),
            std::move(out_format),
            std::move(in_format),
            std::move(timeout_write_buffer),
            std::move(timeout_read_buffer));

        /// Start send data thread
        UDFExecutionContext * ctx_ptr = ctx.get();
        constexpr UInt64 timeout_ms = 5;
        ctx->send_data_thread = ThreadFromGlobalPool([ctx_ptr] {
            while (!ctx_ptr->is_shutdown)
            {
                try
                {
                    std::optional<Block> block = ctx_ptr->input_block_queue.take(timeout_ms);
                    if (block)
                    {
                        ctx_ptr->out_format->setAutoFlush();
                        ctx_ptr->out_format->write(*block);
                        ctx_ptr->write_buffer->next();
                        ctx_ptr->write_buffer->reset();
                    }
                }
                catch (...)
                {
                    bool old_val = false;
                    ctx_ptr->command_is_invalid.compare_exchange_strong(old_val, true);
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        });

        return ctx;
    };

    UDFExecutionContextPtr ctx;
    bool result = udf_ctx_pool->tryBorrowObject(ctx, create_context, configuration.max_command_execution_time_seconds * 1000);

    if (!result)
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get process from pool, max command execution timeout exceeded {} seconds",
            configuration.max_command_execution_time_seconds);

    return ctx;
}

void ShellCommandSourceCoordinator::release(UDFExecutionContextPtr && ctx)
{
    if (ctx->command_is_invalid)
    {
        /// destroy the context
        ctx = nullptr;
        udf_ctx_pool->removeObject();
    }
    else
    {
        /// return subprocess to pool
        udf_ctx_pool->returnObject(std::move(ctx));
    }
}

JavaScriptExecutionContextPtr
ShellCommandSourceCoordinator::getJavaScriptContext(const UserDefinedFunctionConfiguration & config, ContextPtr context)
{
    JavaScriptExecutionContextPtr ctx;

    auto create_context = [&config, &context]() { return std::make_unique<JavaScriptExecutionContext>(config, context); };
    bool result = js_ctx_pool->tryBorrowObject(ctx, create_context, configuration.max_command_execution_time_seconds * 1000);

    if (!result)
        throw Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Could not get Javascript UDF '{}' from pool, it exceeded {} seconds and get timeout",
            config.name,
            configuration.max_command_execution_time_seconds);

    return ctx;
}

void ShellCommandSourceCoordinator::release(JavaScriptExecutionContextPtr && ctx)
{
    js_ctx_pool->returnObject(std::move(ctx));
}

void ShellCommandSourceCoordinator::sendData(UDFExecutionContextPtr & ctx, const Block & block)
{
    if (ctx)
    {
        ctx->input_block_queue.add(block);
        LOG_TRACE(&Poco::Logger::get("ShellCommandSourceCoordinator"), "send {} rows to UDF", block.rows());
    }
}

ColumnPtr ShellCommandSourceCoordinator::pull(UDFExecutionContextPtr & ctx, const DataTypePtr & result_type, size_t result_rows_count)
{
    auto result_column = pull(ctx->in_format, result_type, result_rows_count);
    if (result_column->size() != result_rows_count)
    {
        bool old_val = false;
        ctx->command_is_invalid.compare_exchange_strong(old_val, true);
    }
    return result_column;
}

ColumnPtr ShellCommandSourceCoordinator::pull(InputFormatPtr & in_format, const DataTypePtr & result_type, size_t result_rows_count)
{
    auto result_column = result_type->createColumn();
    result_column->reserve(result_rows_count);

    if (auto * row_fmt = static_cast<IRowInputFormat *>(in_format.get()))
        row_fmt->setMaxSize(result_rows_count);

    Block block = in_format->read(result_rows_count, configuration.command_read_timeout_milliseconds);
    return block.safeGetByPosition(0).column;
}

void ShellCommandSourceCoordinator::stopProcessPool()
{
    if (udf_ctx_pool->borrowedObjectsSize() == 0 && udf_ctx_pool->allocatedObjectsSize() > 0)
        udf_ctx_pool->clearUp();

    js_ctx_pool->clearUp();
}
/// proton: ends

}
