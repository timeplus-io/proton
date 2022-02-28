#include <Interpreters/Context.h>
#include <Interpreters/Streaming/DDLHelper.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <gtest/gtest.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Util/XMLConfiguration.h>

using namespace DB;

ASTPtr queryToAST(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserQuery parser(end);
    return parseQuery(parser, start, end, "", 0, 0);
}

static String queryToJSON(const String & query)
{
    ASTPtr ast = queryToAST(query);

    Poco::JSON::Object::Ptr json_ptr;
    String json_str;
    if (const auto * create = ast->as<ASTCreateQuery>())
        json_str = getJSONFromCreateQuery(*create);
    else if (const auto * alter = ast->as<ASTAlterQuery>())
        json_str = getJSONFromAlterQuery(*alter);

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
    `device`         String,
    `location`       String,
    `cpu_usage`      Float32,
    `timestamp`      DateTime64(3) DEFAULT now64(3),
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(1, 1, rand()))###")),
        ignoreEmptyChars(R"###({
"columns": [{
    "name": "device",
    "nullable": false,
    "type": "String"
}, {
    "name": "location",
    "nullable": false,
    "type": "String"
}, {
    "name": "cpu_usage",
    "nullable": false,
    "type": "Float32"
}, {
    "default": "now64(3)",
    "name": "timestamp",
    "nullable": false,
    "type": "DateTime64(3)"
}, {
    "default": "now()",
    "name": "ttl",
    "nullable": false,
    "type": "DateTime"
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
    `_tp_time`      DateTime(3) DEFAULT now(UTC),
    `_tp_index_time`DateTime(3) DEFAULT now(UTC),
    `_tp_xxxx`      UInt64,
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(2, 1, rand()))###"),
        ignoreEmptyChars(R"###(
{
	"columns": [{
		"default": "now(UTC)",
		"name": "_tp_time",
		"nullable": false,
		"type": "DateTime64(3)"
	}, {
		"default": "now(UTC)",
		"name": "_tp_index_time",
		"nullable": false,
		"type": "DateTime64(3)"
	}, {
		"name": "_tp_xxxx",
		"nullable": false,
		"type": "UInt64"
	}, {
		"default": "now()",
		"name": "ttl",
		"nullable": false,
		"type": "DateTime"
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
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(2, 1, rand())
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
		"type": "DateTime"
	}],
	"name": "tests",
	"order_by_granularity": "H",
	"partition_by_granularity": "D",
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
        queryToJSON(R"###(ALTER STREAM tests ADD COLUMN id UInt32 DEFAULT 20)###"),
        ignoreEmptyChars(R"###({"default": "20", "name": "id", "nullable": false, "type": "UInt32"})###"));

    /// modify column
    EXPECT_EQ(
        queryToJSON(R"###(ALTER STREAM tests MODIFY COLUMN id UInt64 DEFAULT 64)###"),
        ignoreEmptyChars(R"###({"default": "64", "name": "id", "nullable": false, "type": "UInt64"})###"));

    /// modify column add comment
    EXPECT_EQ(
        queryToJSON(R"###(ALTER STREAM tests MODIFY COLUMN id UInt64 DEFAULT 64 COMMENT 'test')###"),
        ignoreEmptyChars(R"###({"comment": "test", "default": "64", "name": "id", "nullable": false, "type": "UInt64"})###"));

    /// rename column
    EXPECT_EQ(queryToJSON(R"###(ALTER STREAM tests RENAME COLUMN id to id1)###"), ignoreEmptyChars(R"###({"name":"id1"})###"));

    /// drop column
    EXPECT_EQ(queryToJSON(R"###(ALTER STREAM tests DROP COLUMN id1)###"), ignoreEmptyChars(R"###({"name": "id1"})###"));
}

DWAL::OpCode getOpCodeFromSQL(const String & query)
{
    auto ast = queryToAST(query);
    if (const auto * alter = ast->as<ASTAlterQuery>())
        return getOpCodeFromQuery(*alter);

    return DWAL::OpCode::MAX_OPS_CODE;
}

TEST(DDLHelper, getOpCodeFromQuery)
{
    /// modify ttl
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests MODIFY TTL ttl + INTERVAL 1 DAY)###"), DWAL::OpCode::ALTER_TABLE);

    /// add column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests ADD COLUMN id UInt32 DEFAULT 20)###"), DWAL::OpCode::CREATE_COLUMN);

    /// modify column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests MODIFY COLUMN id UInt64 DEFAULT 64)###"), DWAL::OpCode::ALTER_COLUMN);

    /// rename column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests RENAME COLUMN id to id1)###"), DWAL::OpCode::ALTER_COLUMN);

    /// drop column
    EXPECT_EQ(getOpCodeFromSQL(R"###(ALTER STREAM tests DROP COLUMN id1)###"), DWAL::OpCode::DELETE_COLUMN);
}

TEST(DDLHelper, getAlterTableParamOpCode)
{
    /// modify ttl
    EXPECT_EQ(getAlterTableParamOpCode({{"query_method", Poco::Net::HTTPRequest::HTTP_POST}}), DWAL::OpCode::ALTER_TABLE);

    /// add column
    EXPECT_EQ(
        getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_POST}}), DWAL::OpCode::CREATE_COLUMN);

    /// modify column
    EXPECT_EQ(
        getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_PATCH}}), DWAL::OpCode::ALTER_COLUMN);

    /// drop column
    EXPECT_EQ(
        getAlterTableParamOpCode({{"column", "test"}, {"query_method", Poco::Net::HTTPRequest::HTTP_DELETE}}), DWAL::OpCode::DELETE_COLUMN);
}

TEST(DDLHelper, prepareCreateQueryForDistributedMergeTree)
{
    /// add _tp_time and _tp_index_time
    ASTPtr ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `device`         String,
    `location`       String,
    `cpu_usage`      Float32,
    `timestamp`      DateTime64(3) DEFAULT now64(3),
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(1, 1, rand())
)###");
    auto * create = ast->as<ASTCreateQuery>();
    prepareCreateQueryForDistributedMergeTree(*create);
    EXPECT_EQ(create->columns_list->columns->children.size(), 7);

    ASTFunction * order_by = create->storage->order_by->as<ASTFunction>();
    EXPECT_EQ(order_by->name, "to_start_of_hour");

    ASTFunction * partition_by = create->storage->partition_by->as<ASTFunction>();
    EXPECT_EQ(partition_by->name, "to_YYYYMMDD");

    /// throw exception if try to add _tp_xxx column
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `_tp_time`      DateTime(3) DEFAULT now(UTC),
    `_tp_index_time`DateTime(3) DEFAULT now(UTC),
    `_tp_xxxx`      UInt64,
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(2, 1, rand()))###");
    create = ast->as<ASTCreateQuery>();
    EXPECT_THROW(prepareCreateQueryForDistributedMergeTree(*create), DB::Exception);

    /// ignore input 'order_by' and 'partition_by'
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(2, 1, rand())
PARTITION BY ttl
ORDER BY ttl
TTL ttl + to_interval_day(1)
)###");
    create = ast->as<ASTCreateQuery>();
    prepareCreateQueryForDistributedMergeTree(*create);
    order_by = create->storage->order_by->as<ASTFunction>();
    EXPECT_EQ(order_by->name, "to_start_of_hour");

    partition_by = create->storage->partition_by->as<ASTFunction>();
    EXPECT_EQ(partition_by->name, "to_YYYYMMDD");

    /// add event_time_column
    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `timestamp`      DateTime64(3) DEFAULT now64(3),
    `ttl`            DateTime DEFAULT now()
) ENGINE = DistributedMergeTree(1, 1, rand())
SETTINGS event_time_column = 'to_start_of_hour(timestamp)'
)###");
    create = ast->as<ASTCreateQuery>();

    prepareCreateQueryForDistributedMergeTree(*create);
    EXPECT_EQ(ignoreEmptyChars(queryToString(*create)), ignoreEmptyChars(R"###(
CREATE STREAM default.tests (
  `timestamp` DateTime64(3) DEFAULT now64(3),
  `ttl` DateTime DEFAULT now(),
  `_tp_time` DateTime64(3,'UTC') DEFAULT to_start_of_hour(timestamp) CODEC(DoubleDelta(), LZ4()),
  `_tp_index_time` DateTime64(3,'UTC') CODEC(DoubleDelta(), LZ4())
) ENGINE = DistributedMergeTree(1, 1, rand())
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time) SETTINGS event_time_column='to_start_of_hour(timestamp)')###"));
}

TEST(DDLHelper, prepareEngine)
{
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config(new Poco::Util::XMLConfiguration());
    global_context->setConfig(config);

    ASTPtr ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            DateTime DEFAULT now()
))###");
    auto * create = ast->as<ASTCreateQuery>();

    prepareEngine(*create, global_context);
    EXPECT_EQ(ignoreEmptyChars(queryToString(*create->storage)), ignoreEmptyChars(R"###(ENGINE = DistributedMergeTree(1, 1, rand()))###"));

    ast = queryToAST(R"###(
CREATE STREAM default.tests
(
    `ttl`            DateTime DEFAULT now()
)
SETTINGS shards=2, replicas=1, sharding_expr='hash(ttl)'
)###");
    create = ast->as<ASTCreateQuery>();
    prepareEngine(*create, global_context);
    EXPECT_EQ(
        ignoreEmptyChars(queryToString(*create->storage)),
        ignoreEmptyChars(R"###(ENGINE=DistributedMergeTree(2,1,hash(ttl))SETTINGSshards=2,replicas=1,sharding_expr='hash(ttl)')###"));
}
