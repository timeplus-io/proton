// Copyright 2014 MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <atomic>
#include <sstream>
#include <type_traits>
#include <utility>

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/logger.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

namespace mongocxx {
namespace v_noabi {

namespace {

log_level convert_log_level(::mongoc_log_level_t mongoc_log_level) {
    switch (mongoc_log_level) {
        case MONGOC_LOG_LEVEL_ERROR:
            return log_level::k_error;
        case MONGOC_LOG_LEVEL_CRITICAL:
            return log_level::k_critical;
        case MONGOC_LOG_LEVEL_WARNING:
            return log_level::k_warning;
        case MONGOC_LOG_LEVEL_MESSAGE:
            return log_level::k_message;
        case MONGOC_LOG_LEVEL_INFO:
            return log_level::k_info;
        case MONGOC_LOG_LEVEL_DEBUG:
            return log_level::k_debug;
        case MONGOC_LOG_LEVEL_TRACE:
            return log_level::k_trace;
        default:
            MONGOCXX_UNREACHABLE;
    }
}

void null_log_handler(::mongoc_log_level_t, const char*, const char*, void*) {}

void user_log_handler(::mongoc_log_level_t mongoc_log_level,
                      const char* log_domain,
                      const char* message,
                      void* user_data) {
    (*static_cast<logger*>(user_data))(convert_log_level(mongoc_log_level),
                                       stdx::string_view{log_domain},
                                       stdx::string_view{message});
}

// A region of memory that acts as a sentintel value indicating that an instance object is being
// destroyed. We only care about the address of this object, never its contents.
typename std::aligned_storage<sizeof(instance), alignof(instance)>::type sentinel;

std::atomic<instance*> current_instance{nullptr};
static_assert(std::is_standard_layout<decltype(current_instance)>::value,
              "Must be standard layout");
static_assert(std::is_trivially_destructible<decltype(current_instance)>::value,
              "Must be trivially destructible");

}  // namespace

class instance::impl {
   public:
    impl(std::unique_ptr<logger> logger) : _user_logger(std::move(logger)) {
        libmongoc::init();
        if (_user_logger) {
            libmongoc::log_set_handler(user_log_handler, _user_logger.get());
            // The libmongoc namespace mocking system doesn't play well with varargs
            // functions, so we use a bare mongoc_log call here.
            mongoc_log(MONGOC_LOG_LEVEL_INFO, "mongocxx", "libmongoc logging callback enabled");
        } else {
            libmongoc::log_set_handler(null_log_handler, nullptr);
        }

        // Despite the name, mongoc_handshake_data_append *prepends* the platform string.
        // mongoc_handshake_data_append does not add a delimitter, so include the " / " in the
        // argument for consistency with the driver_name, and driver_version.
        std::stringstream platform;
        long stdcxx = __cplusplus;
#ifdef _MSVC_LANG
        // Prefer _MSVC_LANG to report the supported C++ standard with MSVC.
        // The __cplusplus macro may be incorrect. See:
        // https://devblogs.microsoft.com/cppblog/msvc-now-correctly-reports-__cplusplus/
        stdcxx = _MSVC_LANG;
#endif
        platform << "CXX=" << MONGOCXX_COMPILER_ID << " " << MONGOCXX_COMPILER_VERSION << " "
                 << "stdcxx=" << stdcxx << " / ";
        libmongoc::handshake_data_append(
            "mongocxx", MONGOCXX_VERSION_STRING, platform.str().c_str());
    }

    ~impl() {
        // If we had a user logger, remove it so that it can't be used by the driver after it goes
        // out of scope
        if (_user_logger) {
            libmongoc::log_set_handler(null_log_handler, nullptr);
        }

// Under ASAN, we don't want to clean up libmongoc, because it causes libraries to become
// unloaded, and then ASAN sees non-rooted allocations that it consideres leaks. These are
// also inscrutable, because the stack refers into an unloaded library, which ASAN can't
// report. Note that this only works if we have built mongoc so that it doesn't do its
// unfortunate automatic invocation of 'cleanup'.
#if !__has_feature(address_sanitizer)
        libmongoc::cleanup();
#endif
    }

    const std::unique_ptr<logger> _user_logger;
};

instance::instance() : instance(nullptr) {}

instance::instance(std::unique_ptr<logger> logger) {
    instance* expected = nullptr;

    if (!current_instance.compare_exchange_strong(expected, this)) {
        throw logic_error{error_code::k_cannot_recreate_instance};
    }

    _impl = stdx::make_unique<impl>(std::move(logger));
}

instance::instance(instance&&) noexcept = default;
instance& instance::operator=(instance&&) noexcept = default;

instance::~instance() {
    current_instance.store(reinterpret_cast<instance*>(&sentinel));
    _impl.reset();
}

instance& instance::current() {
    if (!current_instance.load()) {
        static instance the_instance;
    }

    instance* curr = current_instance.load();

    if (curr == reinterpret_cast<instance*>(&sentinel)) {
        throw logic_error{error_code::k_instance_destroyed};
    }

    return *curr;
}

}  // namespace v_noabi
}  // namespace mongocxx
