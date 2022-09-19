DROP STREAM IF EXISTS test_startsWith;
create stream test_startsWith (a string) Engine = MergeTree PARTITION BY tuple() ORDER BY a;
INSERT INTO test_startsWith (a) values ('a'), ('abcd'), ('bbb'), (''), ('abc');
SELECT count() from test_startsWith where startsWith(a, 'a') settings force_primary_key=1;
SELECT count() from test_startsWith where startsWith(a, 'abc') settings force_primary_key=1;
DROP STREAM test_startsWith;
