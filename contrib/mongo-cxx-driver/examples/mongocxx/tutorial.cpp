// Compile with: c++ --std=c++11 tutorial.cpp $(pkg-config --cflags --libs libmongocxx)

// The following is a formatted copy from the tutorial https://mongocxx.org/mongocxx-v3/tutorial/.

#include <cstdint>
#include <iostream>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

// Redefine assert after including headers. Release builds may undefine the assert macro and result
// in -Wunused-variable warnings.
#if defined(NDEBUG) || !defined(assert)
#undef assert
#define assert(stmt)                                                                         \
    do {                                                                                     \
        if (!(stmt)) {                                                                       \
            std::cerr << "Assert on line " << __LINE__ << " failed: " << #stmt << std::endl; \
            abort();                                                                         \
        }                                                                                    \
    } while (0)
#endif

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

int main() {
    mongocxx::instance instance{};  // This should be done only once.
    mongocxx::uri uri("mongodb://localhost:27017");
    mongocxx::client client(uri);

    auto db = client["mydb"];
    auto collection = db["test"];

    // Create a Document
    {
        auto doc_value = make_document(
            kvp("name", "MongoDB"),
            kvp("type", "database"),
            kvp("count", 1),
            kvp("versions", make_array("v6.0", "v5.0", "v4.4", "v4.2", "v4.0", "v3.6")),
            kvp("info", make_document(kvp("x", 203), kvp("y", 102))));

        auto doc_view = doc_value.view();
        auto element = doc_view["name"];
        assert(element.type() == bsoncxx::type::k_string);
        auto name = element.get_string().value;  // For C++ driver version < 3.7.0, use get_utf8()
        assert(0 == name.compare("MongoDB"));
    }

    // Insert One Document: { "i": 0 }
    {
        auto insert_one_result = collection.insert_one(make_document(kvp("i", 0)));
        assert(insert_one_result);  // Acknowledged writes return results.
        auto doc_id = insert_one_result->inserted_id();
        assert(doc_id.type() == bsoncxx::type::k_oid);
    }

    // Insert Multiple Documents: { "i": 1 } and { "i": 2 }
    {
        std::vector<bsoncxx::document::value> documents;
        documents.push_back(make_document(kvp("i", 1)));
        documents.push_back(make_document(kvp("i", 2)));

        auto insert_many_result = collection.insert_many(documents);
        assert(insert_many_result);  // Acknowledged writes return results.
        auto doc0_id = insert_many_result->inserted_ids().at(0);
        auto doc1_id = insert_many_result->inserted_ids().at(1);
        assert(doc0_id.type() == bsoncxx::type::k_oid);
        assert(doc1_id.type() == bsoncxx::type::k_oid);
    }

    // Find a Single Document in a Collection
    {
        auto find_one_result = collection.find_one({});
        if (find_one_result) {
            // Do something with *find_one_result
        }
        assert(find_one_result);
    }

    // Find All Documents in a Collection
    {
        auto cursor_all = collection.find({});
        for (auto doc : cursor_all) {
            // Do something with doc
            assert(doc["_id"].type() == bsoncxx::type::k_oid);
        }
    }

    // Print All Documents in a Collection
    {
        auto cursor_all = collection.find({});
        std::cout << "collection " << collection.name()
                  << " contains these documents:" << std::endl;
        for (auto doc : cursor_all) {
            std::cout << bsoncxx::to_json(doc, bsoncxx::ExtendedJsonMode::k_relaxed) << std::endl;
        }
        std::cout << std::endl;
    }

    // Get A Single Document That Matches a Filter
    {
        auto find_one_filtered_result = collection.find_one(make_document(kvp("i", 0)));
        if (find_one_filtered_result) {
            // Do something with *find_one_filtered_result
        }
    }

    // Get All Documents That Match a Filter
    {
        auto cursor_filtered =
            collection.find(make_document(kvp("i", make_document(kvp("$gt", 0), kvp("$lte", 2)))));
        for (auto doc : cursor_filtered) {
            // Do something with doc
            assert(doc["_id"].type() == bsoncxx::type::k_oid);
        }
    }

    // Update a Single Document
    {
        auto update_one_result =
            collection.update_one(make_document(kvp("i", 0)),
                                  make_document(kvp("$set", make_document(kvp("foo", "bar")))));
        assert(update_one_result);  // Acknowledged writes return results.
        assert(update_one_result->modified_count() == 1);
    }

    // Update Multiple Documents
    {
        auto update_many_result =
            collection.update_many(make_document(kvp("i", make_document(kvp("$gt", 0)))),
                                   make_document(kvp("$set", make_document(kvp("foo", "buzz")))));
        assert(update_many_result);  // Acknowledged writes return results.
        assert(update_many_result->modified_count() == 2);
    }

    // Delete a Single Document
    {
        auto delete_one_result = collection.delete_one(make_document(kvp("i", 0)));
        assert(delete_one_result);  // Acknowledged writes return results.
        assert(delete_one_result->deleted_count() == 1);
    }

    // Delete All Documents That Match a Filter
    {
        auto delete_many_result =
            collection.delete_many(make_document(kvp("i", make_document(kvp("$gt", 0)))));
        assert(delete_many_result);  // Acknowledged writes return results.
        assert(delete_many_result->deleted_count() == 2);
    }

    // Create Indexes
    {
        auto index_specification = make_document(kvp("i", 1));
        collection.create_index(std::move(index_specification));
    }

    // Drop collection to clean up.
    collection.drop();
}