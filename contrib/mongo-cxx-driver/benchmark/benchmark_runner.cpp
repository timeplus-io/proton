// Copyright 2017 MongoDB Inc.
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

#include "benchmark_runner.hpp"

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>

#include "bson/bson_encoding.hpp"
#include "multi_doc/bulk_insert.hpp"
#include "multi_doc/find_many.hpp"
#include "multi_doc/gridfs_download.hpp"
#include "multi_doc/gridfs_upload.hpp"
#include "parallel/gridfs_multi_export.hpp"
#include "parallel/gridfs_multi_import.hpp"
#include "parallel/json_multi_export.hpp"
#include "parallel/json_multi_import.hpp"
#include "single_doc/find_one_by_id.hpp"
#include "single_doc/insert_one.hpp"
#include "single_doc/run_command.hpp"
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/types.hpp>

namespace benchmark {

// The task sizes and iteration numbers come from the Driver Perfomance Benchmarking Reference Doc.
benchmark_runner::benchmark_runner(std::set<benchmark_type> types) : _types{types} {
    using bsoncxx::stdx::make_unique;

    // Bson microbenchmarks
    _microbenches.push_back(
        make_unique<bson_encoding>("TestFlatEncoding", 75.31, "extended_bson/flat_bson.json"));
    _microbenches.push_back(
        make_unique<bson_encoding>("TestDeepEncoding", 19.64, "extended_bson/deep_bson.json"));
    _microbenches.push_back(
        make_unique<bson_encoding>("TestFullEncoding", 57.34, "extended_bson/full_bson.json"));
    // TODO CXX-1241: Add bson_decoding equivalents.

    // Single doc microbenchmarks
    _microbenches.push_back(make_unique<run_command>());
    _microbenches.push_back(make_unique<find_one_by_id>("single_and_multi_document/tweet.json"));
    _microbenches.push_back(make_unique<insert_one>(
        "TestSmallDocInsertOne", 2.75, iterations, "single_and_multi_document/small_doc.json"));
    _microbenches.push_back(make_unique<insert_one>(
        "TestLargeDocInsertOne", 27.31, 10, "single_and_multi_document/large_doc.json"));

    // Multi doc microbenchmarks
    _microbenches.push_back(make_unique<find_many>("single_and_multi_document/tweet.json"));
    _microbenches.push_back(make_unique<bulk_insert>(
        "TestSmallDocBulkInsert", 2.75, iterations, "single_and_multi_document/small_doc.json"));
    _microbenches.push_back(make_unique<bulk_insert>(
        "TestLargeDocBulkInsert", 27.31, 10, "single_and_multi_document/large_doc.json"));
    // CXX-2794: Disable GridFS benchmarks due to long runtime
    // _microbenches.push_back(
    //     make_unique<gridfs_upload>("single_and_multi_document/gridfs_large.bin"));
    // _microbenches.push_back(
    //     make_unique<gridfs_download>("single_and_multi_document/gridfs_large.bin"));

    // Parallel microbenchmarks
    _microbenches.push_back(make_unique<json_multi_import>("parallel/ldjson_multi"));
    _microbenches.push_back(make_unique<json_multi_export>("parallel/ldjson_multi"));
    // CXX-2794: Disable GridFS benchmarks due to long runtime
    // _microbenches.push_back(make_unique<gridfs_multi_import>("parallel/gridfs_multi"));
    // _microbenches.push_back(make_unique<gridfs_multi_export>("parallel/gridfs_multi"));

    // Need to remove some
    if (!_types.empty()) {
        for (auto&& it = _microbenches.begin(); it != _microbenches.end();) {
            const std::set<benchmark_type>& tags = (*it)->get_tags();
            std::set<benchmark_type> intersect;
            std::set_intersection(tags.begin(),
                                  tags.end(),
                                  _types.begin(),
                                  _types.end(),
                                  std::inserter(intersect, intersect.begin()));

            if (intersect.empty()) {
                _microbenches.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void benchmark_runner::run_microbenches() {
    _start_time = std::chrono::system_clock::now();
    for (std::unique_ptr<microbench>& bench : _microbenches) {
        std::cout << "Starting " << bench->get_name() << "..." << std::endl;

        bench->run();

        auto score = bench->get_results();

        std::cout << bench->get_name() << ": "
                  << static_cast<double>(score.get_percentile(50).count()) / 1000.0
                  << " second(s) | " << score.get_score() << " MB/s" << std::endl
                  << std::endl;
    }
    _end_time = std::chrono::system_clock::now();
}

double benchmark_runner::calculate_average(benchmark_type tag) {
    std::uint32_t count = 0;
    double total = 0.0;
    for (std::unique_ptr<microbench>& bench : _microbenches) {
        if (bench->has_tag(tag)) {
            count++;
            total += bench->get_results().get_score();
        }
    }
    return total / static_cast<double>(count);
}

double benchmark_runner::calculate_bson_bench_score() {
    return calculate_average(benchmark_type::bson_bench);
}

double benchmark_runner::calculate_single_bench_score() {
    return calculate_average(benchmark_type::single_bench);
}

double benchmark_runner::calculate_multi_bench_score() {
    return calculate_average(benchmark_type::multi_bench);
}

double benchmark_runner::calculate_parallel_bench_score() {
    return calculate_average(benchmark_type::parallel_bench);
}

double benchmark_runner::calculate_read_bench_score() {
    return calculate_average(benchmark_type::read_bench);
}

double benchmark_runner::calculate_write_bench_score() {
    return calculate_average(benchmark_type::write_bench);
}

double benchmark_runner::calculate_driver_bench_score() {
    return (calculate_read_bench_score() + calculate_write_bench_score()) / 2.0;
}

void benchmark_runner::write_scores() {
    double read = -1;
    double write = -1;

    using namespace bsoncxx;
    using builder::basic::sub_document;

    auto doc = builder::basic::document{};
    doc.append(kvp("info", [](sub_document subdoc) {
        subdoc.append(kvp("test_name", "C++ Driver microbenchmarks"));
    }));

    auto write_time =
        [](const std::chrono::time_point<std::chrono::system_clock> t) -> std::string {
        std::time_t t1 = std::chrono::system_clock::to_time_t(t);
        std::ostringstream oss;
        oss << std::put_time(std::gmtime(&t1), "%Y-%m-%dT%H:%M:%S") << "+00:00";
        return oss.str();
    };
    doc.append(kvp("created_at", write_time(_start_time)));
    doc.append(kvp("completed_at", write_time(_end_time)));
    doc.append(kvp("artifacts", builder::basic::make_array()));

    auto metrics_array = builder::basic::array{};
    std::cout << std::endl << "Composite benchmarks:" << std::endl << "===========" << std::endl;

    std::cout << "Individual microbenchmark scores:" << std::endl << "===========" << std::endl;
    for (auto&& bench : _microbenches) {
        auto& score = bench->get_results();
        const auto bench_time = static_cast<double>(score.get_percentile(50).count()) / 1000.0;

        std::cout << bench->get_name() << ": " << bench_time << " seconds | " << score.get_score()
                  << " MB/s" << std::endl;

        auto metric_doc = builder::basic::document{};
        metric_doc.append(kvp("name", bench->get_name()));
        metric_doc.append(kvp("type", "THROUGHPUT"));
        metric_doc.append(kvp("value", score.get_score()));
        metrics_array.append(metric_doc);
    }
    doc.append(kvp("metrics", metrics_array));
    doc.append(kvp("sub_tests", builder::basic::make_array()));

    auto print_comp = [this, &read, &write](benchmark_type type) {
        double avg = calculate_average(type);

        if (read < 0 && type == benchmark_type::read_bench) {
            read = avg;
        } else if (write < 0 && type == benchmark_type::write_bench) {
            write = avg;
        }

        std::cout << type_names.at(type) << " " << avg << " MB/s" << std::endl;
    };

    if (!_types.empty()) {
        for (auto&& type : _types) {
            print_comp(type);
        }
    } else {
        for (auto&& pair : names_types) {
            print_comp(pair.second);
        }
    }

    if (read > 0 && write > 0) {
        std::cout << "DriverBench: " << (read + write) / 2.0 << " MB/s" << std::endl;
    }

    std::ofstream os{"results.json"};
    os << '[' << bsoncxx::to_json(doc.view()) << ']';
}
}  // namespace benchmark
