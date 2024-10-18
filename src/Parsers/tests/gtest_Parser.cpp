#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateQuery.h>
/// proton: starts
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserTablePropertiesQuery.h>
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
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('a, b')"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]')"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT b"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') EXCEPT (a, b)"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY a, b, c"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY *",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY *"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT a"
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)",
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * EXCEPT (a, b)"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserOptimizeQuery_FAIL, ParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<ParserAlterCommand>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>
        {
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') APPLY(x)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY COLUMNS('[a]') REPLACE(y)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * APPLY(x)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY * REPLACE(y)",
            },
            {
                "OPTIMIZE TABLE table_name DEDUPLICATE BY db.a, db.b, db.c",
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
                                 }
                             })));

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
/// proton: ends
