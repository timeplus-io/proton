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

#include "score_recorder.hpp"

#include <algorithm>
#include <stdexcept>

namespace benchmark {

score_recorder::score_recorder(double task_size)
    : _execution_time{0}, _sorted{false}, _task_size{task_size} {}

const std::chrono::milliseconds& score_recorder::get_execution_time() const {
    return _execution_time;
}

void benchmark::score_recorder::start_sample() {
    _last_start = std::chrono::high_resolution_clock::now();
}

void score_recorder::end_sample() {
    std::chrono::time_point<std::chrono::high_resolution_clock> end =
        std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - _last_start);

    _samples.push_back(duration);
    _sorted = false;
    _execution_time += duration;
}

const std::chrono::milliseconds& score_recorder::get_percentile(unsigned long n) {
    if (_samples.empty()) {
        throw std::runtime_error("No samples recorded yet");
    }

    if (!_sorted) {
        std::sort(_samples.begin(), _samples.end());
        _sorted = true;
    }

    if (_samples.size() == 1) {
        return _samples[0];
    }

    return _samples[(_samples.size() * n / 100) - 1];
}

double score_recorder::get_score() {
    return _task_size / (static_cast<double>(get_percentile(50).count()) * .001);
}
}  // namespace benchmark
