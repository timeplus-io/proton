// Copyright 2016 MongoDB Inc.
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

#include <cstdlib>
#include <memory>

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/logger.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

namespace {

// The mongocxx::instance constructor and destructor initialize and shut down the driver,
// respectively. Therefore, a mongocxx::instance must be created before using the driver and
// must remain alive for as long as the driver is in use.
//
// This example demonstrates how one might keep a heap allocated mongocxx::instance and
// mongocxx::pool object associated in a way that allows dynamic configuration. Most of the examples
// simply create a stack allocated mongocxx::instance object, which is fine for small examples, but
// real programs will typically require shared access to a commonly configured instance and
// connection pool across different translation units and components. By providing a singleton which
// owns the instance and pool objects, we can defer configuration until we are ready to use MongoDB,
// and share the instance and pool objects between multiple functions.

class mongo_access {
   public:
    static mongo_access& instance() {
        static mongo_access instance;
        return instance;
    }

    void configure(std::unique_ptr<mongocxx::instance> instance,
                   std::unique_ptr<mongocxx::pool> pool) {
        _instance = std::move(instance);
        _pool = std::move(pool);
    }

    using connection = mongocxx::pool::entry;

    connection get_connection() {
        return _pool->acquire();
    }

    bsoncxx::stdx::optional<connection> try_get_connection() {
        return _pool->try_acquire();
    }

   private:
    mongo_access() = default;

    std::unique_ptr<mongocxx::instance> _instance = nullptr;
    std::unique_ptr<mongocxx::pool> _pool = nullptr;
};

}  // namespace

// The 'configure' and 'do_work' functions use the same mongocxx::instance and mongocxx::pool
// objects by way of the mongo_access singleton.

void configure(mongocxx::uri uri) {
    class noop_logger : public mongocxx::logger {
       public:
        virtual void operator()(mongocxx::log_level,
                                bsoncxx::stdx::string_view,
                                bsoncxx::stdx::string_view) noexcept {}
    };

    auto instance =
        bsoncxx::stdx::make_unique<mongocxx::instance>(bsoncxx::stdx::make_unique<noop_logger>());

    mongo_access::instance().configure(std::move(instance),
                                       bsoncxx::stdx::make_unique<mongocxx::pool>(std::move(uri)));
}

bool do_work() {
    auto connection = mongo_access::instance().get_connection();
    if (!connection)
        return false;
    return true;
}

int main(int argc, char* argv[]) {
    auto uri = mongocxx::uri{(argc >= 2) ? argv[1] : mongocxx::uri::k_default_uri};
    configure(std::move(uri));
    return do_work() ? EXIT_SUCCESS : EXIT_FAILURE;
}
