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

#pragma once

#include <chrono>
#include <ctime>
#include <vector>

namespace benchmark {
class score_recorder {
   public:
    score_recorder() = delete;

    score_recorder(double task_size);

    //
    // Starts the timer for a single iteration (sample) of the benchmark.
    //
    void start_sample();

    //
    // Stops the timer and stores the time of the previous sample.
    //
    // @note
    //   This method should only be run once after a call to start_sample().
    //
    void end_sample();

    //
    // Returns the cumulative wall clock execution time of all samples that have been run.
    //
    // @return
    //  The cumulative execution time.
    //
    const std::chrono::milliseconds& get_execution_time() const;

    //
    // Gets the nth percentile sample runtime.
    //
    // @return
    //   The "nth" percentile recorded sample time.
    //
    // @exception
    //   A runtime error is thrown if this method is called before any samples have been recorded.
    //
    // @note
    //   This method should only be called after all samples are completed.
    //
    const std::chrono::milliseconds& get_percentile(unsigned long n);

    //
    // Gets the score for this benchmark.
    //
    // @return
    //  The score for this benchmark in MB/s.
    //
    // @exception
    //   A runtime error is thrown if this method is called before any samples have been recorded.
    //
    // @note
    //  This method should only be called after all samples are completed.
    //
    double get_score();

   private:
    std::chrono::time_point<std::chrono::high_resolution_clock> _last_start;

    std::chrono::milliseconds _execution_time;

    bool _sorted;

    double _task_size;

    std::vector<std::chrono::milliseconds> _samples;
};
}  // namespace benchmark