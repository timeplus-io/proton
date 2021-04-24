#include <IO/ReadBuffer.h>
#include <IO/copyData.h>
#include <common/SimpleJSON.h>

#include <gtest/gtest.h>

#include <unordered_map>


using namespace DB;
using namespace std;

using ExpectInfo = std::unordered_map<String, size_t>;

void checkJSON(SimpleJSON & obj, ExpectInfo & info)
{
    const char * end = obj.dataEnd();
    const char * begin = obj.data();
    const char * pre = begin;
    String name;
    std::unordered_map<String, std::shared_ptr<ReadBuffer>> buffers;
    for (auto it = obj.begin(); it != obj.end(); ++it)
    {
        if (pre != begin)
        {
            size_t size = it.data() - pre;
//            std::cout << "Value:" << String{pre, size} << "\t";
//            std::cout << "Buffer size: " << size << std::endl;
            buffers.emplace(name, std::make_shared<ReadBuffer>(const_cast<char *>(pre), size));
        }
        name = it.getName();
//        std::cout << "Name: " << name << "\t";
        auto val = it.getValue();
        pre = val.data();
    }

    if (pre != begin)
    {
        size_t size = end - pre;
//        std::cout << "Value:" << String{pre, size} << "\t";
//        std::cout << "Buffer size: " << size << std::endl;
        buffers.emplace(name, std::make_shared<ReadBuffer>(const_cast<char *>(pre), size));
    }

    for (auto idx = info.begin(); idx != info.end(); ++idx)
    {
        auto it = buffers.find(idx->first);
        EXPECT_TRUE(it != buffers.end());
        EXPECT_EQ(it->second->internalBuffer().size(), idx->second);
    }
}

void checkJSON(String & req, ExpectInfo & info)
{
    SimpleJSON obj{req.c_str(), req.c_str() + req.size()};
    checkJSON(obj, info);
}

TEST(SimpleJSON, Search)
{
    /// white space in begin, end, before key, et al
    String req = "{\n"
                 "    \"columns\" : [\"a\"],\n"
                 "    \"data\" : [[ 20], [21 ]]\n"
                 "}";
    ExpectInfo info{{"columns", 6}, {"data", 16}};
    checkJSON(req, info);
    info.clear();

    /// json element types
    req = R"###({
    "enrichment": {
        "time_extraction_type": "json_path",
        "time_extraction_rule": "log.time"
    },
    "data":[
        {"_raw": "{\"log\":{\"time\":\"2021-03-21 00:10:23\"}}", "host": 1, "source": "app1", "sourcetype":"log" },
        {"_raw": "{\"log\":{\"time\":\"2021-03-22 00:12:23\"}}", "host": 1, "source": "app2", "sourcetype":"log"},
        {"_raw": "{\"log\":{\"time\":\"2021-03-23 00:12:23\"}}", "host": 2, "source": ["app3", "app4"], "sourcetype":"log", "_time": "2021-04-01 08:08:08"}
    ],
    "string" : "abc",
    "int" : 343,
    "bool": true,
    "null": null
}
)###";
    info.emplace("enrichment", 96);
    info.emplace("data", 395);
    info.emplace("string", 6);
    info.emplace("int", 4);
    info.emplace("bool", 5);
    info.emplace("null", 7);
    checkJSON(req, info);
    info.clear();

    /// Number Check
    req = R"###({"int": -1, "float": -2.3})###";
    info.emplace("int", 3);
    info.emplace("float", 5);
    checkJSON(req, info);
    info.clear();

    /// Array Check
    req = R"###({
"arr_string": ["a", "b", "c", 132],
"arr_number": [1, -1, 2.4, 0, -2.3, "ab"],
"arr_bool": [true, false, true],
"arr_null": [ null, null, null],
"arr_empty": [],
"arr_arr": [[1,2], [3,4]],
"arr_obj": [{"prop1": "a"}, {"propb": 123}]
})###";
    info.emplace("arr_string", 21);
    info.emplace("arr_number", 28);
    info.emplace("arr_bool", 20);
    info.emplace("arr_null", 22);
    info.emplace("arr_empty", 3);
    info.emplace("arr_arr", 15);
    info.emplace("arr_obj", 34);
    checkJSON(req, info);
    info.clear();

    /// Object Check
    req = R"###({
        "obj": {
          "field1": {
            "a": 1,
            "b": "abc"
          }
       }
    })###";
    info.emplace("obj", 49);
    checkJSON(req, info);
}
