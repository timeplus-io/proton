#include <iomanip>
#include <iostream>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <gtest/gtest.h>

using namespace DB;

String convertQueryToFormat(String query,bool one_line)
{
    DB::ASTPtr ast;
    const char * begin = query.data();
    const char * end = query.data() + query.size();
    ParserQuery parser(end, false);
    ast = parseQuery(parser, begin, end, "", 262144, 1000);
    return queryToString(ast, one_line);
}

TEST(indent, stream_name)
{
    //test format 'one_line'=false;
    String query("with it1 as (with it2 as (select * from numbers(10)) select * from it2) select user.id from user,it1 where it1.number=user.id");
    String expected_ans("WITH it1 AS\n  (\n    WITH it2 AS\n      (\n        SELECT \n          *\n        FROM \n          numbers(10)\n      )\n    SELECT \n      *\n    FROM \n      it2\n  )\nSELECT \n  user.id\nFROM \n  user,it1\nWHERE \n  it1.number = user.id");
    String ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    //test DISTINCT  JOIN  WHERE  PARTITION BY  GROUP BY  HAVING ORDER BY  LIMIT  EMIT  SETTINGS
    query = "with it1 as (select * from numbers(10)) select distinct user.id from user join it1 on user.id=it1.number group by user.id having id>0 order by id limit 10 emit last 1s settings query_mode='stream'";
    expected_ans = "WITH it1 AS\n  (\n    SELECT \n      *\n    FROM \n      numbers(10)\n  )\nSELECT DISTINCT \n  user.id\nFROM \n  user\nINNER JOIN it1 ON user.id = it1.number\nGROUP BY \n  user.id\nHAVING \n  id > 0\nORDER BY \n  id ASC\nLIMIT 10\nEMIT LAST 1s\nSETTINGS \n  query_mode = 'stream'";
    ans = convertQueryToFormat(query,false);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    //test format 'one_line'=true;
    query = "with it1 as (with it2 as (select * from numbers(10)) select * from it2) select user.id from user,it1 where it1.number=user.id";
    expected_ans = "WITH it1 AS (WITH it2 AS (SELECT * FROM numbers(10)) SELECT * FROM it2) SELECT user.id FROM user,it1 WHERE it1.number = user.id";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

    query = "with it1 as (select * from numbers(10)) select distinct user.id from user join it1 on user.id=it1.number group by user.id having id>0 order by id limit 10 emit last 1s settings query_mode='stream'";
    expected_ans = "WITH it1 AS (SELECT * FROM numbers(10)) SELECT DISTINCT user.id FROM user INNER JOIN it1 ON user.id = it1.number GROUP BY user.id HAVING id > 0 ORDER BY id ASC LIMIT 10 EMIT LAST 1s SETTINGS query_mode = 'stream'";
    ans = convertQueryToFormat(query,true);
    EXPECT_STREQ(expected_ans.c_str(), ans.c_str());

}
