// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/view_or_value.hpp>
#include <bsoncxx/test/catch.hh>

namespace {
using namespace bsoncxx;

using bsoncxx::to_json;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("document::view_or_value", "[bsoncxx::document::view_or_value]") {
    auto empty = make_document();
    auto doc = make_document(kvp("a", 1));
    auto json = to_json(doc.view());

    SECTION("can be default-constructed") {
        document::view_or_value variant{};
        REQUIRE(to_json(variant) == to_json(empty));
    }

    SECTION("can be constructed with a view") {
        document::view_or_value variant{doc.view()};

        SECTION("can be used as a view") {
            REQUIRE(to_json(variant) == json);
        }

        SECTION("can be copy constructed") {
            document::view_or_value copied{variant};
            REQUIRE(to_json(copied) == json);
        }

        SECTION("can be copy assigned") {
            document::view_or_value copied{empty.view()};
            {
                document::view_or_value temp{doc.view()};
                copied = temp;
            }
            REQUIRE(to_json(copied) == json);
        }

        SECTION("can be move constructed") {
            document::view_or_value temp{doc.view()};
            document::view_or_value moved{std::move(temp)};
            REQUIRE(to_json(moved) == json);
            REQUIRE(to_json(temp) == to_json(empty));
        }

        SECTION("can be move assigned") {
            document::view_or_value moved{variant.view()};
            {
                document::view_or_value temp{doc.view()};
                moved = std::move(temp);
                REQUIRE(to_json(temp) == to_json(empty));
            }
            REQUIRE(to_json(moved) == json);
        }
    }

    SECTION("can be constructed with a value") {
        auto move_doc = doc;
        document::view_or_value variant{std::move(move_doc)};

        SECTION("can be used as a view") {
            REQUIRE(to_json(variant) == json);
        }

        SECTION("can be copy constructed") {
            document::view_or_value copied{variant};
            REQUIRE(to_json(copied) == json);
        }

        SECTION("can be copy assigned") {
            document::view_or_value copied{empty};
            {
                auto temp_doc = doc;
                document::view_or_value temp{std::move(temp_doc)};
                copied = temp;
            }
            REQUIRE(to_json(copied) == json);
        }

        SECTION("can be move constructed") {
            document::view_or_value temp{doc.view()};
            document::view_or_value moved{std::move(temp)};
            REQUIRE(to_json(moved) == json);
            REQUIRE(to_json(temp) == to_json(empty));
        }

        SECTION("can be move assigned") {
            document::view_or_value moved{empty};
            {
                auto temp_doc = doc;
                document::view_or_value temp{std::move(temp_doc)};
                moved = std::move(temp);
                REQUIRE(to_json(temp) == to_json(empty));
            }
            REQUIRE(to_json(moved) == json);
        }
    }

    SECTION("Can be compared to another view_or_value") {
        SECTION("Compares equal with equal views, regardless of ownership") {
            document::value temp{doc};
            document::view_or_value a{std::move(temp)};
            document::view_or_value b{doc.view()};

            REQUIRE(b == a);
        }

        SECTION("Compares inequal with different views") {
            auto temp_a = make_document(kvp("a", 1));
            auto temp_b = make_document(kvp("b", 1));
            document::view_or_value a{std::move(temp_a)};
            document::view_or_value b{std::move(temp_b)};

            REQUIRE(a != b);
        }
    }

    SECTION("Can be compared to a plain View") {
        auto bad_doc = make_document(kvp("blah", 1));
        document::view_or_value variant(doc.view());
        REQUIRE(variant == doc.view());
        REQUIRE(doc.view() == variant);
        REQUIRE(variant != bad_doc.view());
        REQUIRE(bad_doc.view() != variant);
    }

    SECTION("Can be compared to a plain Value") {
        auto bad_doc = make_document(kvp("blah", 1));
        document::view_or_value variant(doc.view());
        REQUIRE(variant == doc);
        REQUIRE(doc == variant);
        REQUIRE(variant != bad_doc);
        REQUIRE(bad_doc != variant);
    }
}

TEST_CASE("string::document::view_or_value", "[bsoncxx::string::view_or_value]") {
    SECTION("can be constructed with a moved-in std::string") {
        std::string name{"Sally"};
        string::view_or_value sally{std::move(name)};
    }

    SECTION("can be constructed with a const std::string&") {
        std::string name{"Jonny"};
        string::view_or_value jonny{name};

        SECTION("is non-owning") {
            REQUIRE(jonny == "Jonny");
            name[1] = 'e';
            REQUIRE(jonny == "Jenny");
        }
    }

    SECTION("can be constructed with a const char*") {
        std::string name = "Julia";
        string::view_or_value julia{name.c_str()};

        SECTION("is non-owning") {
            REQUIRE(julia == "Julia");
            name[4] = 'o';
            REQUIRE(julia == "Julio");
        }
    }

    SECTION("can be constructed with a stdx::string_view") {
        std::string name{"Mike"};
        stdx::string_view name_view(name);
        string::view_or_value mike{name_view};

        SECTION("is non-owning") {
            REQUIRE(mike == "Mike");
            name[3] = 'a';
            REQUIRE(mike == "Mika");
        }
    }

    SECTION("can be compared to a std::string") {
        std::string name{"Theo"};
        std::string other_name{"Tad"};
        string::view_or_value theo{name};

        REQUIRE(theo == name);
        REQUIRE(name == theo);
        REQUIRE(theo != other_name);
        REQUIRE(other_name != theo);
    }

    SECTION("can be compared to a const char*") {
        string::view_or_value bess{"Bess"};

        REQUIRE(bess == "Bess");
        REQUIRE("Bess" == bess);
        REQUIRE(bess != "Betty");
        REQUIRE("Betty" != bess);
    }

    SECTION("can be compared to a stdx::string_view") {
        stdx::string_view name{"Carlin"};
        stdx::string_view other_name{"Cailin"};
        string::view_or_value carlin{name};

        REQUIRE(carlin == name);
        REQUIRE(name == carlin);
        REQUIRE(carlin != other_name);
        REQUIRE(other_name != carlin);
    }

    SECTION("has a terminated() method that returns a copy") {
        SECTION("when owning, copy is non-owning") {
            std::string name{"Rob"};
            string::view_or_value original{std::move(name)};

            string::view_or_value copy{original.terminated()};
            REQUIRE(copy == "Rob");

            char* original_name = const_cast<char*>(original.data());
            original_name[0] = 'B';

            // copy should also reflect the change
            REQUIRE(copy == "Bob");
        }

        SECTION("when non-owning, copy is owning") {
            std::string name{"Sam"};
            string::view_or_value original{name};

            string::view_or_value copy{original.terminated()};
            REQUIRE(copy == "Sam");

            char* original_name = const_cast<char*>(original.data());
            original_name[0] = 'P';

            // copy should not reflect the change
            REQUIRE(copy == "Sam");

            SECTION("and null-terminated") {
                REQUIRE(copy.data()[3] == '\0');
            }
        }
    }
}
}  // namespace
