#include <Interpreters/Context.h>
#include <Interpreters/Streaming/DDLHelper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <gtest/gtest.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/XMLConfiguration.h>

DB::ASTPtr queryToAST(const DB::String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    DB::ParserQuery parser(end);
    return parseQuery(parser, start, end, "", 0, 0);
}

static String queryToJSON(const String & query)
{
    DB::ASTPtr ast = queryToAST(query);

    Poco::JSON::Object::Ptr json_ptr;
    String json_str;
    if (const auto * create = ast->as<DB::ASTCreateQuery>())
        json_str = DB::Streaming::getJSONFromCreateQuery(*create);
    else if (const auto * alter = ast->as<DB::ASTAlterQuery>())
        json_str = DB::Streaming::getJSONFromAlterQuery(*alter);

    return json_str;
}


std::string ignoreEmptyChars(std::string str)
{
    str.erase(std::remove(str.begin(), str.end(), '\n'), str.end());
    str.erase(std::remove(str.begin(), str.end(), ' '), str.end());
    str.erase(std::remove(str.begin(), str.end(), '\t'), str.end());
    return str;
}

TEST(DDLHelper, getJSONFromCreateQuery)
{
    /// normal case
    EXPECT_EQ(
        ignoreEmptyChars(queryToJSON(R"###(
CREATE STREAM default.tests
(
    `device`         string,
    `location`       string,
    `cpu_usage`      float32,
    `timestamp`      datetime64(3) DEFAULT now64(3),
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(1, 1, rand()))###")),
        ignoreEmptyChars(R"###({
"columns": [{
    "name": "device",
    "nullable": false,
    "type": "string"
}, {
    "name": "location",
    "nullable": false,
    "type": "string"
}, {
    "name": "cpu_usage",
    "nullable": false,
    "type": "float32"
}, {
    "default": "now64(3)",
    "name": "timestamp",
    "nullable": false,
    "type": "datetime64(3)"
}, {
    "default": "now()",
    "name": "ttl",
    "nullable": false,
    "type": "datetime"
}],
"name": "tests",
"order_by_granularity": "H",
"partition_by_granularity": "D",
"replication_factor": 1,
"shard_by_expression": "rand()",
"shards": 1
})###"));

    /// ignore _tp_xxxx columns
    EXPECT_EQ(
        queryToJSON(R"###(
CREATE STREAM default.tests
(
    `_tp_time`      datetime(3) DEFAULT now(UTC),
    `_tp_index_time`datetime(3) DEFAULT now(UTC),
    `_tp_xxxx`      uint64,
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(2, 1, rand()))###"),
        ignoreEmptyChars(R"###(
{
	"columns": [{
		"default": "now(UTC)",
		"name": "_tp_time",
		"nullable": false,
		"type": "datetime64(3)"
	}, {
		"default": "now(UTC)",
		"name": "_tp_index_time",
		"nullable": false,
		"type": "datetime64(3)"
	}, {
		"name": "_tp_xxxx",
		"nullable": false,
		"type": "uint64"
	}, {
		"default": "now()",
		"name": "ttl",
		"nullable": false,
		"type": "datetime"
	}],
	"name": "tests",
	"order_by_granularity": "H",
	"partition_by_granularity": "D",
	"replication_factor": 1,
	"shard_by_expression": "rand()",
	"shards": 2
})###"));

    /// ignore 'order by' and 'partition by'
    EXPECT_EQ(
        ignoreEmptyChars(queryToJSON(R"###(
CREATE STREAM default.tests
(
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(2, 1, rand())
PARTITION BY ttl
ORDER BY ttl
TTL ttl + to_interval_day(1)
)###")),
        ignoreEmptyChars(R"###(
{
	"columns": [{
		"default": "now()",
		"name": "ttl",
		"nullable": false,
		"type": "datetime"
	}],
	"name": "tests",
	"order_by_expression": "ttl",
	"partition_by_expression": "ttl",
	"replication_factor": 1,
	"shard_by_expression": "rand()",
	"shards": 2,
    "ttl_expression":"ttl + to_interval_day(1)"
})###"));
}

TEST(DDLHelper, getJSONFromAlterQuery)
{
    /// modify ttl
    EXPECT_EQ(
        ignoreEmptyChars(queryToJSON(R"###(ALTER STREAM tests MODIFY TTL ttl + INTERVAL 1 DAY)###")),
        ignoreEmptyChars(R"###({"ttl_expression": "ttl + INTERVAL1DAY"})###"));

    /// add column
    EXPECT_EQ(
        queryToJSON(R"###(ALTER STREAM tests ADD COLUMN id uint32 DEFAULT 20)###"),
        ignoreEmptyChars(R"###({"default": "20", "name": "id", "nullable": false, "type": "uint32"})###"));

    /// modify column
    EXPECT_EQ(
        queryToJSON(R"###(ALTER STREAM tests MODIFY COLUMN id uint64 DEFAULT 64)###"),
        ignoreEmptyChars(R"###({"default": "64", "name": "id", "nullable": false, "type": "uint64"})###"));

    /// modify column add comment
    EXPECT_EQ(
        queryToJSON(R"###(ALTER STREAM tests MODIFY COLUMN id uint64 DEFAULT 64 COMMENT 'test')###"),
        ignoreEmptyChars(R"###({"comment": "test", "default": "64", "name": "id", "nullable": false, "type": "uint64"})###"));

    /// rename column
    EXPECT_EQ(queryToJSON(R"###(ALTER STREAM tests RENAME COLUMN id to id1)###"), ignoreEmptyChars(R"###({"name":"id1"})###"));

    /// drop column
    EXPECT_EQ(queryToJSON(R"###(ALTER STREAM tests DROP COLUMN id1)###"), ignoreEmptyChars(R"###({"name": "id1"})###"));
}

nlog::OpCode getOpCodeFromSQL(const String & query)
{
    auto ast = queryToAST(query);
    if (const auto * alter = ast->as<DB::ASTAlterQuery>())
        return DB::Streaming::getOpCodeFromQuery(*alter);

    return nlog::OpCode::MAX_OPS_CODE;
}

TEST(DDLHelper, getOpCodeFromQuery)
{
    /// modify ttl
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests MODIFY TTL ttl + INTERVAL 1 DAY)###"), nlog::OpCode::ALTER_TABLE);

    /// add column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests ADD COLUMN id uint32 DEFAULT 20)###"), nlog::OpCode::CREATE_COLUMN);

    /// modify column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests MODIFY COLUMN id uint64 DEFAULT 64)###"), nlog::OpCode::ALTER_COLUMN);

    /// rename column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests RENAME COLUMN id to id1)###"), nlog::OpCode::ALTER_COLUMN);

    /// drop column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests DROP COLUMN id1)###"), nlog::OpCode::DELETE_COLUMN);
}

TEST(DDLHelper, getAlterTableParamOpCode)
{
    /// modify ttl
    EXPECT_EQ(DB::Streaming::getAlterTableParamOpCode({{"query_method", Poco::Net::HTTPRequest::HTTP_POST}}), nlog::OpCode::ALTER_TABLE);

    /// add column
    EXPECT_EQ(
        DB::Streaming::getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_POST}}), nlog::OpCode::CREATE_COLUMN);

    /// modify column
    EXPECT_EQ(
        DB::Streaming::getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_PATCH}}), nlog::OpCode::ALTER_COLUMN);

    /// drop column
    EXPECT_EQ(
        DB::Streaming::getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_DELETE}}), nlog::OpCode::DELETE_COLUMN);
}

TEST(DDLHelper, checkAndPrepareCreateQueryForStream)
{
    /// add _tp_time and _tp_index_time
    DB::ASTPtr ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `device`         string,
    `location`       string,
    `cpu_usage`      float32,
    `timestamp`      datetime64(3) DEFAULT now64(3),
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(1, 1, rand())
)###");
    auto * create = ast->as<DB::ASTCreateQuery>();
    DB::Streaming::checkAndPrepareCreateQueryForStream(*create);
    EXPECT_EQ(create->columns_list->columns->children.size(), 7);

    DB::ASTFunction * order_by = create->storage->order_by->as<DB::ASTFunction>();
    EXPECT_EQ(order_by->name, "to_start_of_hour");

    DB::ASTFunction * partition_by = create->storage->partition_by->as<DB::ASTFunction>();
    EXPECT_EQ(partition_by->name, "to_YYYYMMDD");

    /// throw exception if try to add _tp_xxx column
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `_tp_time`      datetime(3) DEFAULT now(UTC),
    `_tp_index_time`datetime(3) DEFAULT now(UTC),
    `_tp_xxxx`      uint64,
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(2, 1, rand()))###");
    create = ast->as<DB::ASTCreateQuery>();
    EXPECT_THROW(DB::Streaming::checkAndPrepareCreateQueryForStream(*create), DB::Exception);

    /// ignore input 'order_by' and 'partition_by'
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(2, 1, rand())
PARTITION BY ttl
ORDER BY ttl
TTL ttl + to_interval_day(1)
)###");
    create = ast->as<DB::ASTCreateQuery>();
    DB::Streaming::checkAndPrepareCreateQueryForStream(*create);
    order_by = create->storage->order_by->as<DB::ASTFunction>();
    EXPECT_EQ(order_by->name, "to_start_of_hour");

    partition_by = create->storage->partition_by->as<DB::ASTFunction>();
    EXPECT_EQ(partition_by->name, "to_YYYYMMDD");

    /// add event_time_column
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `timestamp`      datetime64(3) DEFAULT now64(3),
    `ttl`            datetime DEFAULT now()
) ENGINE = Stream(1, 1, rand())
SETTINGS event_time_column = 'to_start_of_hour(timestamp)'
)###");
    create = ast->as<DB::ASTCreateQuery>();

    DB::Streaming::checkAndPrepareCreateQueryForStream(*create);
    EXPECT_EQ(ignoreEmptyChars(queryToString(*create)), ignoreEmptyChars(R"###(
CREATE STREAM default.tests (
  `timestamp` datetime64(3) DEFAULT now64(3),
  `ttl` datetime DEFAULT now(),
  `_tp_time` datetime64(3,'UTC') DEFAULT to_start_of_hour(timestamp) CODEC(DoubleDelta(), LZ4()),
  `_tp_index_time` datetime64(3,'UTC') CODEC(DoubleDelta(), LZ4())
) ENGINE = Stream(1, 1, rand())
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time) SETTINGS event_time_column='to_start_of_hour(timestamp)')###"));
}

TEST(DDLHelper, prepareEngine)
{
    auto shared_context = DB::Context::createShared();
    auto global_context = DB::Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config(new Poco::Util::XMLConfiguration());
    global_context->setConfig(config);

    DB::ASTPtr ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            datetime DEFAULT now()
))###");
    auto * create = ast->as<DB::ASTCreateQuery>();

    DB::Streaming::prepareEngine(*create, global_context);
    EXPECT_EQ(ignoreEmptyChars(queryToString(*create->storage)), ignoreEmptyChars(R"###(ENGINE = Stream(1, 1, rand()))###"));

    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            datetime DEFAULT now()
)
SETTINGS shards=2, replicas=1, sharding_expr='hash(ttl)'
)###");
    create = ast->as<DB::ASTCreateQuery>();
    DB::Streaming::prepareEngine(*create, global_context);
    EXPECT_EQ(
        ignoreEmptyChars(queryToString(*create->storage)),
        ignoreEmptyChars(R"###(ENGINE=Stream(2,1,hash(ttl))SETTINGSshards=2,replicas=1,sharding_expr='hash(ttl)')###"));
}
