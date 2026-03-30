# Unit Testing Reference

## Run commands

```bash
# All tests
./run-unit-tests.sh

# Prefix filter — auto-appends '*' (matches all tests in a class)
./run-unit-tests.sh StorageStream
./run-unit-tests.sh Parser

# Exact case
./run-unit-tests.sh "AccessRights.Union"

# Glob pattern (must quote)
./run-unit-tests.sh "Parser*"

# List matching tests without running
./run-unit-tests.sh -l
./run-unit-tests.sh -l StorageStream

# Write JUnit XML report
./run-unit-tests.sh -o ./tmp/unit_report
```

## Options

| Option | Default | Purpose |
|--------|---------|---------|
| positional arg | — | Filter expression (prefix → appends `*`, exact if contains `.` or `:`) |
| `-f FILTER` | — | Explicit `--gtest_filter` expression (overrides positional) |
| `-l` | — | `--gtest_list_tests`: list matches, skip execution |
| `-o DIR` | — | Write `DIR/unit_test_results.xml` (JUnit) |

Environment override: `UNIT_TEST_BIN` — full path to override binary resolution.

## Direct binary (when not using the script)

```bash
./build/src/stripped/bin/unit_tests_dbms --gtest_filter="StorageStream*"
./build/src/stripped/bin/unit_tests_dbms --gtest_list_tests
```

## Find test names

```bash
# List all suite names
./run-unit-tests.sh -l 2>&1 | grep '^[A-Z]'

# Search for tests matching a keyword
./run-unit-tests.sh -l 2>&1 | grep -i "stream"
```

---

## Writing a new unit test

Unit tests use GoogleTest. Test files live under `src/` next to the code they test.

### Checklist

- [ ] Find or create `src/.../tests/gtest_<module>.cpp`
- [ ] Add `TEST(SuiteName, CaseName)` — suite name = class/module, case = scenario
- [ ] Keep each test self-contained: no shared mutable state between cases
- [ ] Use `EXPECT_*` for non-fatal checks, `ASSERT_*` to stop on failure
- [ ] Verify locally: `./run-unit-tests.sh "SuiteName.CaseName"`

### Minimal example

```cpp
#include <gtest/gtest.h>

TEST(MyFeatureSuite, HandlesEmptyInput)
{
    auto result = myFunction({});
    EXPECT_TRUE(result.empty());
}

TEST(MyFeatureSuite, ProducesCorrectOutput)
{
    auto result = myFunction({1, 2, 3});
    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0], 1);
}
```

### Register the test file in CMake

`unit_tests_dbms` collects test sources via `file(GLOB_RECURSE ... "gtest*.cpp")` in `src/CMakeLists.txt`.
**No manual CMake edit is needed** as long as the file is named `gtest_<module>.cpp` and placed under `src/`.

> Exception: files under subdirectories controlled by feature flags (e.g. `USE_V8`) may need
> an explicit `target_sources` in their local `CMakeLists.txt` — check the surrounding files.
