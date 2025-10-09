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

#include <iostream>
#include <sstream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/test/to_string.hh>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/test/spec/monitoring.hh>
#include <mongocxx/test/spec/unified_tests/assert.hh>
#include <third_party/catch/include/catch.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace spec {

using namespace mongocxx;
using bsoncxx::to_json;

void remove_ignored_command_monitoring_events(apm_checker::event_vector& events,
                                              const std::vector<std::string>& ignore) {
    auto is_ignored = [&](bsoncxx::document::value v) {
        return std::any_of(std::begin(ignore), std::end(ignore), [&](stdx::string_view key) {
            return v.view()["commandStartedEvent"]["command"][key] ||
                   v.view()["commandFailedEvent"]["command"][key] ||
                   v.view()["commandSucceededEvent"]["command"][key];
        });
    };

    events.erase(std::remove_if(events.begin(), events.end(), is_ignored), std::end(events));
}

// commands postfixed with "_unified" are used to support the unified test format.
void apm_checker::compare_unified(bsoncxx::array::view expectations,
                                  entity::map& map,
                                  bool ignore_extra_events) {
    remove_ignored_command_monitoring_events(_events, _ignore);

    CAPTURE(print_all());

    // This will throw an exception on unmatched fields and return true in all other cases.
    auto compare = [&](const bsoncxx::array::element& exp, const bsoncxx::document::view actual) {
        CAPTURE(to_json(actual), bsoncxx::to_string(exp.get_value()));

        // Extra fields are only allowed in root-level documents. Here, each k in keys is treated
        // as its own root-level document, allowing extra fields.
        auto match_events = [&](stdx::string_view event, std::initializer_list<std::string> keys) {
            for (auto&& k : keys)
                if (exp[event][k])
                    assert::matches(actual[event][k].get_value(), exp[event][k].get_value(), map);
        };

        match_events("commandStartedEvent", {"command", "commandName", "databaseName"});
        match_events("commandSucceededEvent", {"reply", "commandName"});
        match_events("commandFailedEvent", {"commandName"});
        return true;
    };

    auto exp_it = expectations.cbegin();
    auto exp_end = expectations.cend();
    auto ev_it = _events.cbegin();
    auto ev_end = _events.cend();
    for (; exp_it != exp_end && ev_it != ev_end; ++exp_it, ++ev_it) {
        compare(*exp_it, *ev_it);
    }
    if (exp_it != exp_end) {
        FAIL_CHECK("Not enough events occurred (Expected "
                   << std::distance(expectations.cbegin(), expectations.cend())
                   << " events, but got " << (_events.size()) << " events)");
    }
    if (!ignore_extra_events && ev_it != ev_end) {
        FAIL_CHECK("Too many events occurred (Expected "
                   << std::distance(expectations.cbegin(), expectations.cend())
                   << " events, but got " << (_events.size()) << " events)");
    }
}

void apm_checker::compare(bsoncxx::array::view expectations,
                          bool allow_extra,
                          const test_util::match_visitor& match_visitor) {
    auto is_ignored = [&](bsoncxx::document::value v) {
        const auto view = v.view();

        // CXX-2155: Sharing a MongoClient for metadata lookup can lead to deadlock in
        // drivers using automatic encryption. Since the C++ driver does not use a separate
        // `client` for listCollections and finds on the key vault, we skip these checks.
        if (view["command_started_event"]["command"]["listCollections"]) {
            const auto db = view["command_started_event"]["command"]["$db"];

            if (db && db.get_string().value == stdx::string_view("keyvault")) {
                return true;
            }
        }

        return std::any_of(std::begin(_ignore), std::end(_ignore), [&](stdx::string_view key) {
            return view["command_started_event"]["command"][key] ||
                   view["command_failed_event"]["command"][key] ||
                   view["command_succeeded_event"]["command"][key];
        });
    };

    auto events_iter = _events.begin();
    _events.erase(std::remove_if(_events.begin(), _events.end(), is_ignored), std::end(_events));
    CAPTURE(print_all());
    for (auto expectation : expectations) {
        auto expected = expectation.get_document().view();
        if (events_iter == _events.end()) {
            FAIL("Not enough events occurred: expected exactly "
                 << std::distance(expectations.begin(), expectations.end()) << " events, but got "
                 << _events.size() << " events");
        }
        REQUIRE_BSON_MATCHES_V(*events_iter, expected, match_visitor);
        events_iter++;
    }

    if (!allow_extra && events_iter != _events.end()) {
        FAIL_CHECK("Too many events occurred: expected exactly "
                   << std::distance(expectations.begin(), expectations.end()) << " events, but got "
                   << _events.size() << " events");
    }
}

void apm_checker::has(bsoncxx::array::view expectations) {
    for (auto expectation : expectations) {
        auto expected = expectation.get_document().view();
        CAPTURE(to_json(expected).c_str(), print_all());
        REQUIRE(std::find_if(_events.begin(), _events.end(), [&](bsoncxx::document::view doc) {
                    return test_util::matches(doc, expected);
                }) != _events.end());
    }
}

bool apm_checker::should_ignore(stdx::string_view command_name) const {
    return std::any_of(std::begin(_ignore),
                       std::end(_ignore),
                       [command_name](stdx::string_view cmp) { return command_name == cmp; });
}

std::string apm_checker::print_all() const {
    std::ostringstream output;
    output << "\n\n";
    output << "APM Checker contents:\n";
    for (const auto& event : _events) {
        output << "APM event: " << bsoncxx::to_json(event) << "\n\n";
    }
    return std::move(output).str();
}

/// A "sensitive" hello is a hello command with the "speculativeAuthenticate" argument set
/// We don't have access to the original command definition here, but (at present) we can
/// detect that it is sensitive by the mongoc library having removed the main body of the
/// command events' requests and responses, thus we check for ".empty()" on that body.
static bool is_hello_cmd_name(stdx::string_view name) {
    return name == stdx::string_view("hello") || name == stdx::string_view("ismaster") ||
           name == stdx::string_view("isMaster");
}
static bool is_sensitive_hello_cmd_event(const events::command_started_event& event) {
    return event.command().empty();
}

static bool is_sensitive_hello_cmd_event(const events::command_succeeded_event& ev) {
    return ev.reply().empty();
}

static bool is_sensitive_hello_cmd_event(const events::command_failed_event& ev) {
    return ev.failure().empty();
}

/**
 * @brief Determine whether 'event' is one of the "sensitive" events.
 *
 * `event` must have a `command_name() const -> stdx::string_view` method. If the
 * event is a 'hello' or 'isMaster' event, it is sensitive if it is a commandStartedEvent
 * and 'speculativeAuthenticate' is provided in the command (See is_sensitive_hello_cmd_event).
 */
template <typename Ev>
static bool is_sensitive_command(const Ev& event) noexcept {
    static stdx::string_view sensitive_commands[] = {
        "authenticate",
        "saslStart",
        "saslContinue",
        "getnonce",
        "createUser",
        "updateUser",
        "gopydbgetnonce",
        "copyDbSaslStart",
        "copydb",
    };
    const bool is_sensitive_cmd_name = std::find(std::begin(sensitive_commands),
                                                 std::end(sensitive_commands),
                                                 event.command_name())  //
                                       != std::end(sensitive_commands);
    if (is_sensitive_cmd_name) {
        return true;
    }
    // Special logic for hello commands
    return is_hello_cmd_name(event.command_name()) && is_sensitive_hello_cmd_event(event);
}

void apm_checker::set_command_started_unified(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_started([&](const events::command_started_event& event) {
        if (!observe_sensitive_events && is_sensitive_command(event)) {
            return;
        }

        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(kvp("commandStartedEvent",
                           make_document(kvp("command", event.command()),
                                         kvp("commandName", event.command_name()),
                                         kvp("databaseName", event.database_name()))));
        this->_events.emplace_back(builder.extract());
    });
}

void apm_checker::set_command_failed_unified(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_failed([&](const events::command_failed_event& event) {
        if (!observe_sensitive_events && is_sensitive_command(event)) {
            return;
        }

        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(
            kvp("commandFailedEvent", make_document(kvp("commandName", event.command_name()))));
        this->_events.emplace_back(builder.extract());
    });
}

void apm_checker::set_command_succeeded_unified(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_succeeded([&](const events::command_succeeded_event& event) {
        if (!observe_sensitive_events && is_sensitive_command(event)) {
            return;
        }

        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(kvp(
            "commandSucceededEvent",
            make_document(kvp("reply", event.reply()), kvp("commandName", event.command_name()))));
        this->_events.emplace_back(builder.extract());
    });
}

void apm_checker::set_command_started(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_started([&](const events::command_started_event& event) {
        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(kvp("command_started_event",
                           make_document(kvp("command", event.command()),
                                         kvp("command_name", event.command_name()),
                                         kvp("operation_id", event.operation_id()),
                                         kvp("database_name", event.database_name()))));
        this->_events.emplace_back(builder.extract());
    });
}

void apm_checker::set_command_failed(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_failed([&](const events::command_failed_event& event) {
        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(kvp("command_failed_event",
                           make_document(kvp("command_name", event.command_name()),
                                         kvp("operation_id", event.operation_id()))));
        this->_events.emplace_back(builder.extract());
    });
}

void apm_checker::set_command_succeeded(options::apm& apm) {
    using namespace bsoncxx::builder::basic;

    apm.on_command_succeeded([&](const events::command_succeeded_event& event) {
        if (should_ignore(event.command_name())) {
            return;
        }

        document builder;
        builder.append(kvp("command_succeeded_event",
                           make_document(kvp("reply", event.reply()),
                                         kvp("command_name", event.command_name()),
                                         kvp("operation_id", event.operation_id()))));
        this->_events.emplace_back(builder.extract());
    });
}

options::apm apm_checker::get_apm_opts(bool command_started_events_only) {
    options::apm opts;

    this->set_command_started(opts);
    if (command_started_events_only)
        return opts;

    this->set_command_failed(opts);
    this->set_command_succeeded(opts);
    return opts;
}

void apm_checker::set_ignore_command_monitoring_event(const std::string& event) {
    this->_ignore.push_back(event);
}

void apm_checker::clear_events() {
    this->_events.clear();
}

void apm_checker::clear() {
    clear_events();
    this->_ignore.clear();
}

}  // namespace spec
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
