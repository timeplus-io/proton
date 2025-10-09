// Copyright 2017-present MongoDB Inc.
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

#include <mongocxx/options/client_session.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

client_session& client_session::causal_consistency(bool causal_consistency) noexcept {
    _causal_consistency = causal_consistency;
    return *this;
}

bool client_session::causal_consistency() const noexcept {
    // Unless causal consistency has been explicitly disabled (i.e. it has a value, and
    // that value is false), we always return true. If snapshot reads are enabled,
    // the invalid setting combination will later be rejected:
    return _causal_consistency.value_or(true);
}

client_session& client_session::snapshot(bool enable_snapshot_reads) noexcept {
    _enable_snapshot_reads = enable_snapshot_reads;
    return *this;
}

bool client_session::snapshot() const noexcept {
    // As per the Snapshot Consistency spec, if there is no value then false is implied:
    return _enable_snapshot_reads.value_or(false);
}

client_session& client_session::default_transaction_opts(transaction default_transaction_opts) {
    _default_transaction_opts = std::move(default_transaction_opts);
    return *this;
}

const stdx::optional<transaction>& client_session::default_transaction_opts() const {
    return _default_transaction_opts;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
