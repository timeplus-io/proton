#pragma once

namespace DB
{
class PipelineExecutor;

struct CheckpointableExecutorHolder
{
    CheckpointableExecutorHolder() = default;

    CheckpointableExecutorHolder(std::weak_ptr<PipelineExecutor> executor_)
        : executor(executor_.lock()), ref_count(std::make_shared<std::atomic_uint32_t>(0))
    {
        if (executor)
            ref_count->fetch_add(1);
    }

    CheckpointableExecutorHolder(CheckpointableExecutorHolder && other)
        : executor(std::move(other.executor)), ref_count(std::move(other.ref_count))
    {
    }

    CheckpointableExecutorHolder(const CheckpointableExecutorHolder & other) : executor(other.executor), ref_count(other.ref_count)
    {
        if (executor)
            ref_count->fetch_add(1);
    }

    void swap(CheckpointableExecutorHolder & other)
    {
        executor.swap(other.executor);
        ref_count.swap(other.ref_count);
    }

    CheckpointableExecutorHolder & operator=(const CheckpointableExecutorHolder & other)
    {
        executor = other.executor;
        ref_count = other.ref_count;
        if (executor)
            ref_count->fetch_add(1);

        return *this;
    }

    ~CheckpointableExecutorHolder()
    {
        if (executor)
            ref_count->fetch_sub(1);
    }

    operator bool() const { return executor != nullptr; }
    const PipelineExecutor & operator*() const { return *executor; }
    const PipelineExecutor * operator->() const { return executor.get(); }
    PipelineExecutor & operator*() { return *executor; }
    PipelineExecutor * operator->() { return executor.get(); }

    bool unique() const { return ref_count->load() == 1; }

private:
    std::shared_ptr<PipelineExecutor> executor;
    std::shared_ptr<std::atomic_uint32_t> ref_count;
};
}
