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

#include <bsoncxx/config/private/prelude.hh>

// The BSONCXX_TEST_API macro is intended to be used for exporting symbols for testing which are not
// normally exported as part of the bsoncxx library.  We currently use this workaround in lieu of
// linking standalone unit tests, in order to expose private symbols for unit testing when we build
// the shared library.

#ifdef BSONCXX_TESTING
#define BSONCXX_TEST_API BSONCXX_API
#else
#define BSONCXX_TEST_API
#endif

#include <bsoncxx/config/private/postlude.hh>
