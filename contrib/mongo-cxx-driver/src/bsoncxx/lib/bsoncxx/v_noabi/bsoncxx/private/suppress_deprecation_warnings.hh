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

// The macros BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN and
// BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END are intended for the use of disabling deprecation
// warnings for a given section of code.
//
// Example usage:
//
//     {
//         some_function();
//         BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN;
//         some_deprecated_function();
//         BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END;
//         some_other_function();
//     }

#ifdef __clang__
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN \
    _Pragma("clang diagnostic push")                \
        _Pragma("clang diagnostic ignored \"-Wdeprecated-declarations\"")
#elif defined __GNUC__
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN \
    _Pragma("GCC diagnostic push") _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#elif defined _MSC_VER
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN \
    __pragma(warning(push)) __pragma(warning(disable : 4996))
#endif

#ifdef __clang__
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END _Pragma("clang diagnostic pop")
#elif defined __GNUC__
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END _Pragma("GCC diagnostic pop")
#elif defined _MSC_VER
#define BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END __pragma(warning(pop))
#endif
