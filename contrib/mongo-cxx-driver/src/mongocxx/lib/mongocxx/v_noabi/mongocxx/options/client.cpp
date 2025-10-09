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

#include <mongocxx/options/client.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

client& client::tls_opts(tls tls_opts) {
    _tls_opts = std::move(tls_opts);
    return *this;
}

const stdx::optional<tls>& client::tls_opts() const {
    return _tls_opts;
}

client& client::ssl_opts(tls ssl_opts) {
    return tls_opts(std::move(ssl_opts));
}

const stdx::optional<tls>& client::ssl_opts() const {
    return tls_opts();
}

client& client::apm_opts(apm apm_opts) {
    _apm_opts = std::move(apm_opts);
    return *this;
}

const stdx::optional<apm>& client::apm_opts() const {
    return _apm_opts;
}

client& client::auto_encryption_opts(auto_encryption auto_encryption_opts) {
    _auto_encrypt_opts = std::move(auto_encryption_opts);
    return *this;
}

const stdx::optional<auto_encryption>& client::auto_encryption_opts() const {
    return _auto_encrypt_opts;
}

client& client::server_api_opts(server_api server_api_opts) {
    _server_api_opts = std::move(server_api_opts);
    return *this;
}

const stdx::optional<server_api>& client::server_api_opts() const {
    return _server_api_opts;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
