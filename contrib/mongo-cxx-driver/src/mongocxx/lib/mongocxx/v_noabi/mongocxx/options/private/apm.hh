// Copyright 2018-present MongoDB Inc.
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

#pragma once

#include <iostream>

#include <mongocxx/options/apm.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

using apm_unique_callbacks =
    std::unique_ptr<mongoc_apm_callbacks_t, decltype(libmongoc::apm_callbacks_destroy)>;

// An APM callback exiting via an exception is documented as being undefined behavior.
// For QoI, terminate the program before allowing the exception to bypass libmongoc code.
template <typename Fn>
static void exception_guard(const char* source, Fn fn) noexcept {
    try {
        fn();
    } catch (...) {
        std::cerr << "fatal error: APM callback " << source << " exited via an exception"
                  << std::endl;
        std::terminate();
    }
}

static void command_started(const mongoc_apm_command_started_t* event) noexcept {
    events::command_started_event started_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_command_started_get_context(event));
    exception_guard(__func__, [&] { context->command_started()(started_event); });
}

static void command_failed(const mongoc_apm_command_failed_t* event) noexcept {
    events::command_failed_event failed_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_command_failed_get_context(event));
    exception_guard(__func__, [&] { context->command_failed()(failed_event); });
}

static void command_succeeded(const mongoc_apm_command_succeeded_t* event) noexcept {
    events::command_succeeded_event succeeded_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_command_succeeded_get_context(event));
    exception_guard(__func__, [&] { context->command_succeeded()(succeeded_event); });
}

static void server_closed(const mongoc_apm_server_closed_t* event) noexcept {
    events::server_closed_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_closed_get_context(event));
    exception_guard(__func__, [&] { context->server_closed()(e); });
}

static void server_changed(const mongoc_apm_server_changed_t* event) noexcept {
    events::server_changed_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_changed_get_context(event));
    exception_guard(__func__, [&] { context->server_changed()(e); });
}

static void server_opening(const mongoc_apm_server_opening_t* event) noexcept {
    events::server_opening_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_opening_get_context(event));
    exception_guard(__func__, [&] { context->server_opening()(e); });
}

static void topology_closed(const mongoc_apm_topology_closed_t* event) noexcept {
    events::topology_closed_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_topology_closed_get_context(event));
    exception_guard(__func__, [&] { context->topology_closed()(e); });
}

static void topology_changed(const mongoc_apm_topology_changed_t* event) noexcept {
    events::topology_changed_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_topology_changed_get_context(event));
    exception_guard(__func__, [&] { context->topology_changed()(e); });
}

static void topology_opening(const mongoc_apm_topology_opening_t* event) noexcept {
    events::topology_opening_event e(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_topology_opening_get_context(event));
    exception_guard(__func__, [&] { context->topology_opening()(e); });
}

static void heartbeat_started(const mongoc_apm_server_heartbeat_started_t* event) noexcept {
    events::heartbeat_started_event started_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_heartbeat_started_get_context(event));
    exception_guard(__func__, [&] { context->heartbeat_started()(started_event); });
}

static void heartbeat_failed(const mongoc_apm_server_heartbeat_failed_t* event) noexcept {
    events::heartbeat_failed_event failed_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_heartbeat_failed_get_context(event));
    exception_guard(__func__, [&] { context->heartbeat_failed()(failed_event); });
}

static void heartbeat_succeeded(const mongoc_apm_server_heartbeat_succeeded_t* event) noexcept {
    events::heartbeat_succeeded_event succeeded_event(static_cast<const void*>(event));
    auto context = static_cast<apm*>(libmongoc::apm_server_heartbeat_succeeded_get_context(event));
    exception_guard(__func__, [&] { context->heartbeat_succeeded()(succeeded_event); });
}

BSON_MAYBE_UNUSED
static apm_unique_callbacks make_apm_callbacks(const apm& apm_opts) {
    mongoc_apm_callbacks_t* callbacks = libmongoc::apm_callbacks_new();

    if (apm_opts.command_started()) {
        libmongoc::apm_set_command_started_cb(callbacks, command_started);
    }

    if (apm_opts.command_failed()) {
        libmongoc::apm_set_command_failed_cb(callbacks, command_failed);
    }

    if (apm_opts.command_succeeded()) {
        libmongoc::apm_set_command_succeeded_cb(callbacks, command_succeeded);
    }

    if (apm_opts.server_closed()) {
        libmongoc::apm_set_server_closed_cb(callbacks, server_closed);
    }

    if (apm_opts.server_changed()) {
        libmongoc::apm_set_server_changed_cb(callbacks, server_changed);
    }

    if (apm_opts.server_opening()) {
        libmongoc::apm_set_server_opening_cb(callbacks, server_opening);
    }

    if (apm_opts.topology_closed()) {
        libmongoc::apm_set_topology_closed_cb(callbacks, topology_closed);
    }

    if (apm_opts.topology_changed()) {
        libmongoc::apm_set_topology_changed_cb(callbacks, topology_changed);
    }

    if (apm_opts.topology_opening()) {
        libmongoc::apm_set_topology_opening_cb(callbacks, topology_opening);
    }

    if (apm_opts.heartbeat_started()) {
        libmongoc::apm_set_server_heartbeat_started_cb(callbacks, heartbeat_started);
    }

    if (apm_opts.heartbeat_failed()) {
        libmongoc::apm_set_server_heartbeat_failed_cb(callbacks, heartbeat_failed);
    }

    if (apm_opts.heartbeat_succeeded()) {
        libmongoc::apm_set_server_heartbeat_succeeded_cb(callbacks, heartbeat_succeeded);
    }

    return {callbacks, libmongoc::apm_callbacks_destroy};
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
