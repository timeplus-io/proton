#pragma once

#include "ExecutorImpl.h"

#include <Core/Block.h>
#include <Core/Streaming/WatermarkInfo.h>

namespace DB::Streaming::Substream
{

template <typename T>
concept HasGetName = requires(T && a) {
                         {
                             a.getName()
                             } -> std::convertible_to<std::string>;
                     };

template <typename T, typename Data>
concept HasCreateSubstream = (std::is_void_v<Data> && requires(T && a, ID && id, char * place) { a.createSubstream(id); })
    || (!std::is_void_v<Data> && requires(T && a, ID && id, char * place) { a.createSubstream(id, place); });

/// Template executor
/// CRTP: inherit Executor
///
/// For example:
/// class Inst : public Executor<Inst, Data>
/// {
///     void createSubstream(const ID & sub_id);  /// Data = void
///  or
///     void createSubstream(const ID & sub_id, char * place); /// replacement new Data
/// };
///
/// Usage:
///  Inst f{header, keys_indices};
///  f.execute(block1);
///  f.execute(block2);
template <typename Derived, typename Data = void>
class Executor
{
public:
    using Self = Executor<Derived, Data>;
    using DataWithID = typename ExecutorImpl<Data>::DataWithID;
    using DataWithIDRows = typename ExecutorImpl<Data>::DataWithIDRows;
    using SubstreamCreator
        = std::conditional_t<std::is_void_v<Data>, std::function<void(const ID)>, std::function<void(const ID, char * place)>>;
    Executor() = default;
    Executor(Block header_, std::vector<size_t> keys_indices_, GroupByKeys groupby_keys_type = GroupByKeys::PARTITION_KEYS)
    {
        init(std::move(header_), std::move(keys_indices_), groupby_keys_type);
    }

    void init(Block header_, std::vector<size_t> keys_indices_, GroupByKeys groupby_keys_type)
    {
        assert(!impl);
        if constexpr (HasGetName<Derived>)
            impl = std::make_unique<ExecutorImpl<Data>>(
                std::move(header_), std::move(keys_indices_), groupby_keys_type, static_cast<Derived &>(*this).getName());
        else
            impl = std::make_unique<ExecutorImpl<Data>>(
                std::move(header_), std::move(keys_indices_), groupby_keys_type, "SubstreamExecutor");

        if constexpr (HasCreateSubstream<Derived, Data>)
        {
            if constexpr (std::is_void_v<Data>)
                substream_creator = [this](const ID & id) { static_cast<Derived &>(*this).createSubstream(id); };
            else
                substream_creator = [this](const ID & id, char * place) { static_cast<Derived &>(*this).createSubstream(id, place); };
        }
    }

    auto & table() { return *impl; }

    template <typename... Fn>
    void execute(const Block & block, Fn &&... fn)
    {
        execute(block.getColumns(), block.rows(), std::forward<Fn>(fn)...);
    }

    template <typename... Fn>
    void execute(const Columns & cols, size_t rows, Fn &&... fn)
    {
        impl->execute(cols, rows, substream_creator, std::forward<Fn>(fn)...);
    }

    /// Return true if found
    template <typename... Fn>
    bool call(const ID & id, Fn &&... fn)
    {
        return impl->executeOne(id, std::forward<Fn>(fn)...);
    }

    template <typename... Fn>
    void forEachValue(Fn &&... fn)
    {
        impl->executeForEachValue(std::forward<Fn>(fn)...);
    }

protected:
    std::unique_ptr<ExecutorImpl<Data>> impl;
    SubstreamCreator substream_creator;
};

}
