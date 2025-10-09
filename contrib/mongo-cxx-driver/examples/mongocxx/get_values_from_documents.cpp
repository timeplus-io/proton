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

#include <iostream>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

using bsoncxx::type;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

// Document model, showing array with nested documents:
//
// {
//     "_id" : ObjectId(...),
//     "messagelist" : [
//       {
//         "uid" : 413098706,
//         "status" : 2,
//         "msg": "..."
//       },
//       ...
//     ]
// }

// Construct a document in the format of 'messagelist'.
bsoncxx::document::value new_message(int64_t uid, int32_t status, std::string msg) {
    return make_document(kvp("uid", uid), kvp("status", status), kvp("msg", msg));
}

// Insert a document into the database.
void insert_test_data(mongocxx::collection& coll) {
    bsoncxx::document::value doc =
        make_document(kvp("messagelist",
                          make_array(new_message(413098706, 3, "Lorem ipsum..."),
                                     new_message(413098707, 2, "Lorem ipsum..."),
                                     new_message(413098708, 1, "Lorem ipsum..."))));

    // Normally, one should check the return value for success.
    coll.insert_one(std::move(doc));
}

// Iterate over contents of messagelist.
void iterate_messagelist(const bsoncxx::document::element& ele) {
    // Check validity and type before trying to iterate.
    if (ele.type() == type::k_array) {
        bsoncxx::array::view subarray{ele.get_array().value};
        for (const bsoncxx::array::element& msg : subarray) {
            // Check correct type before trying to access elements.
            // Only print out fields if they exist; don't report missing fields.
            if (msg.type() == type::k_document) {
                bsoncxx::document::view subdoc = msg.get_document().value;
                bsoncxx::document::element uid = subdoc["uid"];
                bsoncxx::document::element status = subdoc["status"];
                bsoncxx::document::element msg = subdoc["msg"];
                if (uid && uid.type() == type::k_int64) {
                    std::cout << "uid: " << uid.get_int64().value << std::endl;
                }
                if (status && status.type() == type::k_int32) {
                    std::cout << "status: " << status.get_int32().value << std::endl;
                }
                if (msg && msg.type() == type::k_string) {
                    std::cout << "msg: " << msg.get_string().value << std::endl;
                }
            } else {
                std::cout << "Message is not a document" << std::endl;
            }
        }
    } else {
        std::cout << "messagelist is not an array" << std::endl;
    }
}

// Print document parts to standard output.
void print_document(const bsoncxx::document::view& doc) {
    // Extract _id element as a string.
    bsoncxx::document::element id_ele = doc["_id"];
    if (id_ele.type() == type::k_oid) {
        std::string oid = id_ele.get_oid().value.to_string();
        std::cout << "OID: " << oid << std::endl;
    } else {
        std::cout << "Error: _id was not an object ID." << std::endl;
    }

    // Extract "messagelist" element, which could be the 'invalid' (false)
    // element if it doesn't exist.
    bsoncxx::document::element msgs_ele = doc["messagelist"];
    if (msgs_ele) {
        iterate_messagelist(msgs_ele);
    } else {
        std::cout << "Error: messagelist field missing." << std::endl;
    }
}

void iterate_documents(mongocxx::collection& coll) {
    // Execute a query with an empty filter (i.e. get all documents).
    mongocxx::cursor cursor = coll.find({});

    // Iterate the cursor into bsoncxx::document::view objects.
    for (const bsoncxx::document::view& doc : cursor) {
        print_document(doc);
    }
}

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client client{mongocxx::uri{}};
    mongocxx::collection coll = client["test"]["events"];

    coll.drop();
    insert_test_data(coll);
    iterate_documents(coll);
}
