#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
/// proton: starts
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
#include <Parsers/Streaming/ParserEmitQuery.h>
/// proton: ends
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <string_view>

#include <gtest/gtest.h>


namespace
{
using namespace DB;
using namespace std::literals;
}

struct ParserTestCase
{
    const std::string_view input_text;
    const char * expected_ast = nullptr;
};

std::ostream & operator<<(std::ostream & ostr, const std::shared_ptr<IParser> parser)
{
    return ostr << "Parser: " << parser->getName();
}

std::ostream & operator<<(std::ostream & ostr, const ParserTestCase & test_case)
{
    return ostr << "ParserTestCase input: " << test_case.input_text;
}

class ParserTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<IParser>, ParserTestCase>>
{};

TEST_P(ParserTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_NE(nullptr, parser);

    if (expected_ast)
    {
        ASTPtr ast;
        ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
        EXPECT_EQ(expected_ast, serializeAST(*ast->clone(), false));
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
    }
}

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserOptimizeQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('a, b')",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('a, b')"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]')",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]')"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY a, b, c",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY a, b, c"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY *",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY *"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * EXCEPT a",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * EXCEPT a"
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * EXCEPT (a, b)",
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * EXCEPT (a, b)"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery_FAIL, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY",
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * APPLY(x)",
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY * REPLACE(y)",
            },
            {
                "OPTIMIZE STREAM table_name DEDUPLICATE BY db.a, db.b, db.c",
            }
        }
)));


INSTANTIATE_TEST_SUITE_P(ParserAlterCommand_MODIFY_COMMENT, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                // Empty comment value
                "MODIFY COMMENT ''",
                "MODIFY COMMENT ''",
            },
            {
                // Non-empty comment value
                "MODIFY COMMENT 'some comment value'",
                "MODIFY COMMENT 'some comment value'",
            }
        }
)));


INSTANTIATE_TEST_SUITE_P(ParserCreateQuery_DICTIONARY_WITH_COMMENT, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateDictionaryQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            R"sql(CREATE DICTIONARY 2024_dictionary_with_comment
(
    id uint64,
    value string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000)
COMMENT 'Test dictionary with comment';
)sql",
        R"sql(CREATE DICTIONARY `2024_dictionary_with_comment`
(
  `id` uint64,
  `value` string
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'source_table'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(FLAT())
COMMENT 'Test dictionary with comment')sql"
    }}
)));

INSTANTIATE_TEST_SUITE_P(ParserCreateDatabaseQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserCreateQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw')",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')"
        },
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE `tbl`\n(PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n  PARTITION BY toYYYYMM(`created`)\n)"
        },
        {
            "CREATE DATABASE db ENGINE=Foo TABLE OVERRIDE `tbl` (), TABLE OVERRIDE a (COLUMNS (_created DateTime MATERIALIZED now())), TABLE OVERRIDE b (PARTITION BY rand())",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE `tbl`,\nTABLE OVERRIDE `a`\n(\n  COLUMNS\n  (\n    `_created` DateTime MATERIALIZED now()\n  )\n),\nTABLE OVERRIDE `b`\n(\n  PARTITION BY rand()\n)"
        },
        {
            "CREATE DATABASE db ENGINE=MaterializeMySQL('addr:port', 'db', 'user', 'pw') TABLE OVERRIDE tbl (COLUMNS (id UUID) PARTITION BY toYYYYMM(created))",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('addr:port', 'db', 'user', 'pw')\nTABLE OVERRIDE `tbl`\n(\n  COLUMNS\n  (\n    `id` UUID\n  )\n  PARTITION BY toYYYYMM(`created`)\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE tbl (COLUMNS (INDEX foo foo TYPE minmax GRANULARITY 1) PARTITION BY if(_staged = 1, 'staging', toYYYYMM(created)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n  COLUMNS\n  (\n    INDEX foo `foo` TYPE minmax GRANULARITY 1\n  )\n  PARTITION BY if(`_staged` = 1, 'staging', toYYYYMM(`created`))\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE t1 (TTL inserted + INTERVAL 1 MONTH DELETE), TABLE OVERRIDE t2 (TTL `inserted` + INTERVAL 2 MONTH DELETE)",
            "CREATE DATABASE db\nTABLE OVERRIDE `t1`\n(\n  TTL `inserted` + INTERVAL 1 MONTH\n),\nTABLE OVERRIDE `t2`\n(\n  TTL `inserted` + INTERVAL 2 MONTH\n)"
        },
        {
            "CREATE DATABASE db ENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw') TABLE OVERRIDE tab3 (COLUMNS (_staged UInt8 MATERIALIZED 1) PARTITION BY (c3) TTL c3 + INTERVAL 10 minute), TABLE OVERRIDE tab5 (PARTITION BY (c3) TTL c3 + INTERVAL 10 minute)",
            "CREATE DATABASE db\nENGINE = MaterializeMySQL('127.0.0.1:3306', 'db', 'root', 'pw')\nTABLE OVERRIDE `tab3`\n(\n  COLUMNS\n  (\n    `_staged` UInt8 MATERIALIZED 1\n  )\n  PARTITION BY `c3`\n  TTL `c3` + INTERVAL 10 MINUTE\n),\nTABLE OVERRIDE `tab5`\n(\n  PARTITION BY `c3`\n  TTL `c3` + INTERVAL 10 MINUTE\n)"
        },
        {
            "CREATE DATABASE db TABLE OVERRIDE tbl (PARTITION BY toYYYYMM(created) COLUMNS (created DateTime CODEC(Delta)))",
            "CREATE DATABASE db\nTABLE OVERRIDE `tbl`\n(\n  COLUMNS\n  (\n    `created` DateTime CODEC(Delta)\n  )\n  PARTITION BY toYYYYMM(`created`)\n)"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() ",
            "CREATE DATABASE db\nENGINE = Foo"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() ",
            "CREATE DATABASE db\nENGINE = Foo"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE `a`\n(\n  ORDER BY (`id`, `version`)\n)"
        },
        {
            "CREATE DATABASE db ENGINE = Foo() COMMENT 'db comment' TABLE OVERRIDE a (ORDER BY (id, version))",
            "CREATE DATABASE db\nENGINE = Foo\nTABLE OVERRIDE `a`\n(\n  ORDER BY (`id`, `version`)\n)\nCOMMENT 'db comment'"
        }
})));

/// proton: starts. CREATE STREAM test cases
INSTANTIATE_TEST_SUITE_P(ParserCreateStreamQuery, ParserTest,
                         ::testing::Combine(
                             ::testing::Values(std::make_shared<ParserCreateQuery>()),
                             ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
                                 {
                                     "CREATE STREAM tests (`device` string)",
                                     "CREATE STREAM tests\n(\n  `device` string\n)"
                                 }
                             })));
/// proton: ends

/// proton: starts. ALTER STREAM test cases
INSTANTIATE_TEST_SUITE_P(ParserAlterStreamQuery, ParserTest,
                         ::testing::Combine(
                             ::testing::Values(std::make_shared<ParserAlterQuery>()),
                             ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
                                 {
                                     "ALTER STREAM tests MODIFY TTL ttl + INTERVAL 1 DAY",
                                     "ALTER STREAM tests\n  MODIFY TTL ttl + INTERVAL 1 DAY"
                                 },
                                 {
                                     "ALTER stream tests MODIFY COLUMN id uint64 DEFAULT 64",
                                     "ALTER STREAM tests\n  MODIFY COLUMN `id` uint64 DEFAULT 64"
                                 },
                                 {
                                     "ALTER STREAM tests RENAME COLUMN id to id1",
                                     "ALTER STREAM tests\n  RENAME COLUMN id TO id1"
                                 },
                                 {
                                     "ALTER STREAM tests DROP COLUMN id1",
                                     "ALTER STREAM tests\n  DROP COLUMN id1"
                                 },
                                 {
                                     "ALTER STREAM mv MODIFY SETTING logstore_retention_ms=1000, logstore_retention_bytes=1000",
                                     "ALTER STREAM mv\n  MODIFY SETTING logstore_retention_ms = 1000, logstore_retention_bytes = 1000"
                                 },
                                 {
                                     "ALTER STREAM mv RESET SETTING logstore_retention_ms, logstore_retention_bytes",
                                     "ALTER STREAM mv\n  RESET SETTING logstore_retention_ms, logstore_retention_bytes"
                                 },
                                 {
                                     "ALTER STREAM mv MODIFY QUERY SETTING checkpoint_interval=60",
                                     "ALTER STREAM mv\n  MODIFY QUERY SETTING checkpoint_interval = 60"
                                 },
                                 {
                                     "ALTER STREAM mv RESET QUERY SETTING checkpoint_interval",
                                     "ALTER STREAM mv\n  RESET QUERY SETTING checkpoint_interval"
                                 }
                             })));
/// proton: ends

/// proton: starts. DROP STREAM test cases
INSTANTIATE_TEST_SUITE_P(ParserDropStreamQuery, ParserTest,
                         ::testing::Combine(
                             ::testing::Values(std::make_shared<ParserDropQuery>()),
                             ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
                                 {
                                     "DROP STREAM tests",
                                     "DROP STREAM tests"
                                 },
                                 {
                                     "TRUNCATE STREAM tests",
                                     "TRUNCATE STREAM tests"
                                 },
                                 {
                                     "TRUNCATE tests",
                                     "TRUNCATE STREAM tests"
                                 }
                             })));
/// proton: ends

/// proton: starts. `SYSTEM INSTALL/UNINSTALL PYTHON PACKAGE ...` test cases
INSTANTIATE_TEST_SUITE_P(
    ParserSystemPythonPackageQuery,
    ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserSystemQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {"SYSTEM INSTALL PYTHON PACKAGE 'requests'", "SYSTEM INSTALL PYTHON PACKAGE 'requests'"},
            {"SYSTEM INSTALL PYTHON PACKAGE 'requests' '2.32.3' INDEX_URL 'https://pypi.org/simple' EXTRA_INDEX_URL "
             "'https://mirror.example/simple' EXTRA_INDEX_URL 'https://backup.example/simple'",
             "SYSTEM INSTALL PYTHON PACKAGE 'requests' '2.32.3' INDEX_URL 'https://pypi.org/simple' EXTRA_INDEX_URL "
             "'https://mirror.example/simple' EXTRA_INDEX_URL 'https://backup.example/simple'"},
            {"SYSTEM INSTALL PYTHON PACKAGE REQUIREMENTS 'python-slugify==8.0.4' INDEX_URL 'https://pypi.org/simple' EXTRA_INDEX_URL "
             "'https://mirror.example/simple'",
             "SYSTEM INSTALL PYTHON PACKAGE REQUIREMENTS 'python-slugify==8.0.4' INDEX_URL 'https://pypi.org/simple' EXTRA_INDEX_URL "
             "'https://mirror.example/simple'"},
            {"SYSTEM UNINSTALL PYTHON PACKAGE 'requests'", "SYSTEM UNINSTALL PYTHON PACKAGE 'requests'"}})));
/// proton: ends

/// proton: starts. `SYSTEM PAUSE/RESUME/ABORT/RECOVER MATERIALIZED VIEW <db.mv> [PERMANENT]` test cases
INSTANTIATE_TEST_SUITE_P(ParserSystemCommandQueryForMV, ParserTest,
                         ::testing::Combine(
                             ::testing::Values(std::make_shared<ParserSystemQuery>()),
                             ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
                                 {
                                     "SYSTEM PAUSE MATERIALIZED VIEW mv",
                                     "SYSTEM PAUSE MATERIALIZED VIEW mv"
                                 },
                                 {
                                     "SYSTEM PAUSE MATERIALIZED VIEW default.mv",
                                     "SYSTEM PAUSE MATERIALIZED VIEW default.mv"
                                 },
                                 {
                                     "SYSTEM PAUSE MATERIALIZED VIEW default.mv PERMANENT",
                                     "SYSTEM PAUSE MATERIALIZED VIEW default.mv PERMANENT"
                                 },
                                 {
                                     "SYSTEM UNPAUSE MATERIALIZED VIEW mv",
                                     "SYSTEM RESUME MATERIALIZED VIEW mv"
                                 },
                                 {
                                     "SYSTEM UNPAUSE MATERIALIZED VIEW default.mv PERMANENT",
                                     "SYSTEM RESUME MATERIALIZED VIEW default.mv PERMANENT"
                                 },
                                 {
                                     "SYSTEM RESUME MATERIALIZED VIEW mv",
                                     "SYSTEM RESUME MATERIALIZED VIEW mv"
                                 },
                                 {
                                     "SYSTEM RESUME MATERIALIZED VIEW default.mv PERMANENT",
                                     "SYSTEM RESUME MATERIALIZED VIEW default.mv PERMANENT"
                                 },
                                 {
                                     "SYSTEM ABORT MATERIALIZED VIEW mv",
                                     "SYSTEM ABORT MATERIALIZED VIEW mv"
                                 },
                                 {
                                     "SYSTEM ABORT MATERIALIZED VIEW default.mv PERMANENT",
                                     "SYSTEM ABORT MATERIALIZED VIEW default.mv PERMANENT"
                                 },
                                 {
                                     "SYSTEM RECOVER MATERIALIZED VIEW mv",
                                     "SYSTEM RECOVER MATERIALIZED VIEW mv"
                                 },
                                 {
                                     "SYSTEM RECOVER MATERIALIZED VIEW default.mv PERMANENT",
                                     "SYSTEM RECOVER MATERIALIZED VIEW default.mv PERMANENT"
                                 }
                             })));
/// proton: ends


/// proton: start
INSTANTIATE_TEST_SUITE_P(ParserSelectQuery_WithAndWithoutAs, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserSelectQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {
                // aliasing column name without AS
                "SELECT id, name username FROM users",
                "SELECT\n  id, name AS username\nFROM\n  users"
            },
            {
                // aliasing table name without AS
                "SELECT * FROM users u",
                "SELECT\n  *\nFROM\n  users AS u"
            },
            {
                // aliasing subqueries without AS
                "SELECT * FROM (SELECT id, name FROM users) u",
                "SELECT\n  *\nFROM\n  (\n    SELECT\n      id, name\n    FROM\n      users\n  ) AS u"
            },
            {
                // combine multiply aliasing without AS
                "SELECT u.id, u.name uname FROM (SELECT id, name FROM users) u",
                "SELECT\n  u.id, u.name AS uname\nFROM\n  (\n    SELECT\n      id, name\n    FROM\n      users\n  ) AS u"
            },
            {
                // aliasing without AS in ORDER BY
                "SELECT id, name uname FROM users ORDER BY uname",
                "SELECT\n  id, name AS uname\nFROM\n  users\nORDER BY\n  uname ASC"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserSelectQuery_WithAndWithoutAs_FAIL, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserSelectQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {
                // `shuffle` is keyword and cannot be regarded as alias
                "SELECT id, name FROM users shuffle",
                nullptr
            },
            {
                // comma at the end of list is not allowed
                "SELECT id, name username, FROM users",
                nullptr
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserTablePropertiesQuery, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserTablePropertiesQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {
                "SHOW CREATE DATABASE db",
                "SHOW CREATE DATABASE db",
            },
            {
                "SHOW CREATE STREAM test",
                "SHOW CREATE STREAM test",
            },
            {
                "SHOW CREATE STREAM db.test",
                "SHOW CREATE STREAM db.test",
            },
            {
                "SHOW CREATE TABLE db.test",
                "SHOW CREATE STREAM db.test",
            },
            {
                "SHOW CREATE VIEW db.testv",
                "SHOW CREATE VIEW db.testv",
            },
            {
                "SHOW CREATE DICTIONARY db.testd",
                "SHOW CREATE DICTIONARY db.testd",
            },
            {
                "EXISTS DATABASE db",
                "EXISTS DATABASE db",
            },
            {
                "EXISTS STREAM db.test",
                "EXISTS STREAM db.test",
            },
            {
                "EXISTS TABLE db.test",
                "EXISTS STREAM db.test",
            },
            {
                "EXISTS VIEW db.testv",
                "EXISTS VIEW db.testv",
            },
            {
                "EXISTS DICTIONARY db.testd",
                "EXISTS DICTIONARY db.testd",
            }
        }
)));


INSTANTIATE_TEST_SUITE_P(
    ParserEmitQuery,
    ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserEmitQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
            {"EMIT CHANGELOG", "CHANGELOG"},
            {"EMIT STREAM AFTER WINDOW CLOSE", "STREAM AFTER WINDOW CLOSE"},
            {"EMIT AFTER WINDOW CLOSE WITH DELAY 1s", "AFTER WINDOW CLOSE WITH DELAY 1s"},
            {"EMIT AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 5s", "AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 5s"},
            {"EMIT AFTER WINDOW CLOSE AND TIMEOUT 5s", "AFTER WINDOW CLOSE AND TIMEOUT 5s"},
            {"EMIT PERIODIC 1s", "PERIODIC 1s"},
            {"EMIT STREAM PERIODIC 1s REPEAT", "STREAM PERIODIC 1s REPEAT"},
            {"EMIT CHANGELOG PERIODIC 1s WITH DELAY 1s", "CHANGELOG PERIODIC 1s WITH DELAY 1s"},
            {"EMIT DELTA PERIODIC 1s REPEAT WITH DELAY 1s AND TIMEOUT 5s", "DELTA PERIODIC 1s REPEAT WITH DELAY 1s AND TIMEOUT 5s"},
            {"EMIT ON UPDATE", "ON UPDATE"},
            {"EMIT CHANGELOG ON UPDATE WITH BATCH 1s", "CHANGELOG ON UPDATE WITH BATCH 1s"},
            {"EMIT ON UPDATE WITH DELAY 1s", "ON UPDATE WITH DELAY 1s"},
            {"EMIT ON UPDATE WITH BATCH 1s WITH DELAY 1s AND TIMEOUT 5s", "ON UPDATE WITH BATCH 1s WITH DELAY 1s AND TIMEOUT 5s"},
            {"EMIT LAST 1h", "LAST 1h"},
            {"EMIT LAST 1h ON PROCTIME", "LAST 1h ON PROCTIME"},
            {"EMIT STREAM AFTER WATERMARK WITH DELAY 1s", "STREAM AFTER WINDOW CLOSE WITH DELAY 1s"},
            {"EMIT PERIODIC 1s ON UPDATE", "ON UPDATE WITH BATCH 1s"},
            {"EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT AFTER KEY EXPIRE IDENTIFIED BY _tp_time WITH ONLY MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY _tp_time WITH ONLY MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, start_col, end_col) WITH MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, start_col, end_col) WITH MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, true, end_col) WITH MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, true, end_col) WITH MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, true, false) WITH MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY _tp_time WITH MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, start_col, false) WITH MAXSPAN 1s AND TIMEOUT 100s",
             "AFTER SESSION CLOSE IDENTIFIED BY (_tp_time, start_col, false) WITH MAXSPAN 1s AND TIMEOUT 100s"},
            {"EMIT PER EVENT WITH DELAY 1s AND TIMEOUT 5s", "PER EVENT WITH DELAY 1s AND TIMEOUT 5s"},
            {"EMIT TIMEOUT 30s", "TIMEOUT 30s"},
            {"EMIT STREAM TIMEOUT 30s", "STREAM TIMEOUT 30s"}})));
/// proton: end
