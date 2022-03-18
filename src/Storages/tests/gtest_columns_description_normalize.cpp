#include <Storages/ColumnsDescription.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(ColumnsDescription, Normalize)
{
    constexpr auto columns = "columns format version: 1\n"
                             "3 columns:\n"
                             "`a` uint32\n"
                             "`b` string\tDEFAULT\tIf(a = 0, 'true', 'false')\n"
                             "`c` string\tDEFAULT\tcAsT(a, 'string')\n";

    constexpr auto columns_normalized = "columns format version: 1\n"
                                        "3 columns:\n"
                                        "`a` uint32\n"
                                        "`b` string\tDEFAULT\tif(a = 0, 'true', 'false')\n"
                                        "`c` string\tDEFAULT\tcast(a, 'string')\n";

    tryRegisterFunctions();

    ASSERT_EQ(ColumnsDescription::parse(columns), ColumnsDescription::parse(columns_normalized));
}
