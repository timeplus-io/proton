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

#include <mongocxx/options/apm.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

apm& apm::on_command_started(
    std::function<void(const events::command_started_event&)> command_started) {
    _command_started = std::move(command_started);
    return *this;
}

const std::function<void(const events::command_started_event&)>& apm::command_started() const {
    return _command_started;
}

apm& apm::on_command_failed(
    std::function<void(const events::command_failed_event&)> command_failed) {
    _command_failed = std::move(command_failed);
    return *this;
}

const std::function<void(const events::command_failed_event&)>& apm::command_failed() const {
    return _command_failed;
}

apm& apm::on_command_succeeded(
    std::function<void(const events::command_succeeded_event&)> command_succeeded) {
    _command_succeeded = std::move(command_succeeded);
    return *this;
}

const std::function<void(const events::command_succeeded_event&)>& apm::command_succeeded() const {
    return _command_succeeded;
}

apm& apm::on_server_opening(
    std::function<void(const events::server_opening_event&)> server_opening) {
    _server_opening = server_opening;
    return *this;
}

const std::function<void(const events::server_opening_event&)>& apm::server_opening() const {
    return _server_opening;
}

apm& apm::on_server_closed(std::function<void(const events::server_closed_event&)> server_closed) {
    _server_closed = server_closed;
    return *this;
}

const std::function<void(const events::server_closed_event&)>& apm::server_closed() const {
    return _server_closed;
}

apm& apm::on_server_changed(
    std::function<void(const events::server_changed_event&)> server_changed) {
    _server_changed = server_changed;
    return *this;
}

const std::function<void(const events::server_changed_event&)>& apm::server_changed() const {
    return _server_changed;
}

apm& apm::on_topology_opening(
    std::function<void(const events::topology_opening_event&)> topology_opening) {
    _topology_opening = topology_opening;
    return *this;
}

const std::function<void(const events::topology_opening_event&)>& apm::topology_opening() const {
    return _topology_opening;
}

apm& apm::on_topology_closed(
    std::function<void(const events::topology_closed_event&)> topology_closed) {
    _topology_closed = topology_closed;
    return *this;
}

const std::function<void(const events::topology_closed_event&)>& apm::topology_closed() const {
    return _topology_closed;
}

apm& apm::on_topology_changed(
    std::function<void(const events::topology_changed_event&)> topology_changed) {
    _topology_changed = topology_changed;
    return *this;
}

const std::function<void(const events::topology_changed_event&)>& apm::topology_changed() const {
    return _topology_changed;
}

apm& apm::on_heartbeat_started(
    std::function<void(const events::heartbeat_started_event&)> heartbeat_started) {
    _heartbeat_started = std::move(heartbeat_started);
    return *this;
}

const std::function<void(const events::heartbeat_started_event&)>& apm::heartbeat_started() const {
    return _heartbeat_started;
}

apm& apm::on_heartbeat_failed(
    std::function<void(const events::heartbeat_failed_event&)> heartbeat_failed) {
    _heartbeat_failed = std::move(heartbeat_failed);
    return *this;
}

const std::function<void(const events::heartbeat_failed_event&)>& apm::heartbeat_failed() const {
    return _heartbeat_failed;
}

apm& apm::on_heartbeat_succeeded(
    std::function<void(const events::heartbeat_succeeded_event&)> heartbeat_succeeded) {
    _heartbeat_succeeded = std::move(heartbeat_succeeded);
    return *this;
}

const std::function<void(const events::heartbeat_succeeded_event&)>& apm::heartbeat_succeeded()
    const {
    return _heartbeat_succeeded;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
