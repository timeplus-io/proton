#pragma once

#include <Cluster/Common/Error.h>
#include <Common/ErrorCodes.h>

namespace cluster
{
/// `CallResult` is used as the function call result in Raft component.
/// It uses error code style to carry the error for function call.
/// We prefer use error code in Raft component since it is less verbose
/// than to handling exceptions and may be more performant.
template <typename T>
struct CallResult
{
    CallResult() = default;

    explicit CallResult(T result_) : result(std::move(result_)) { }
    explicit CallResult(int32_t error_code_) : error_code(error_code_) { }
    CallResult(T result_, int32_t error_code_) : result(std::move(result_)), error_code(error_code_) { }

    bool hasError() const noexcept { return error_code != 0; }

    auto errorString() const noexcept { return DB::ErrorCodes::getName(error_code); }

    T result{};

    int32_t error_code = 0;
};

/// `CallResultV` is verbose version of CallResult, with detail error message
template <typename T>
struct CallResultV
{
    CallResultV() = default;

    explicit CallResultV(T result_) : result(std::move(result_)) { }

    explicit CallResultV(Error && err_) : err(std::move(err_)) { }

    /// Sometimes, we like to return `result` as well as error
    CallResultV(T result_, Error && err_) : result(std::move(result_)), err(std::move(err_)) { }

    CallResultV(int32_t error_code_, std::string && error_message_) : err(error_code_, std::move(error_message_)) { }

    bool hasError() const noexcept { return err.hasError(); }

    auto errorString() const { return err.string(); }

    void swap(CallResultV<T> & other) noexcept
    {
        std::swap(result, other.result);
        err.swap(other.err);
    }

    T result{};

    Error err;
};

template <>
struct CallResultV<void>
{
    CallResultV() = default;

    explicit CallResultV(Error && err_) : err(std::move(err_)) { }

    CallResultV(int32_t error_code_, std::string && error_message_) : err(error_code_, std::move(error_message_)) { }

    bool hasError() const noexcept { return err.hasError(); }

    auto errorString() const { return err.string(); }

    void swap(CallResultV<void> & other) noexcept { err.swap(other.err); }

    Error err;
};
}
