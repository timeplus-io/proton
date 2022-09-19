-- Tags: no-parallel

create stream old_syntax_01071_test (date date, id uint8) ENGINE = MergeTree(date, id, 8192);
ALTER STREAM old_syntax_01071_test ADD INDEX  id_minmax id TYPE minmax GRANULARITY 1; -- { serverError 36 }
create stream new_syntax_01071_test (date date, id uint8) ENGINE = MergeTree() ORDER BY id;
ALTER STREAM new_syntax_01071_test ADD INDEX  id_minmax id TYPE minmax GRANULARITY 1;
DETACH TABLE new_syntax_01071_test;
ATTACH TABLE new_syntax_01071_test;
DROP STREAM IF EXISTS old_syntax_01071_test;
DROP STREAM IF EXISTS new_syntax_01071_test;
