DROP DATABASE IF EXISTS test_01656;

CREATE DATABASE test_01656;
USE test_01656;

DROP STREAM IF EXISTS table_key;
DROP STREAM IF EXISTS table_with_enum;

create stream table_key (keycol uint16) ENGINE = MergeTree() ORDER BY (keycol) PARTITION BY tuple();

create stream table_with_enum (keycol uint16, enum_col Enum8('First' = 1,'Second' = 2))
    ENGINE = MergeTree() ORDER BY (keycol) PARTITION BY tuple();

INSERT INTO table_key VALUES (1), (2), (3);
INSERT INTO table_with_enum VALUES (2, 'Second'), (4, 'Second');

SET join_algorithm = 'hash';

SELECT keycol, enum_col FROM table_key LEFT JOIN table_with_enum USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_key FULL JOIN table_with_enum USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_key RIGHT JOIN table_with_enum USING (keycol) ORDER BY keycol;

SELECT keycol, enum_col FROM table_with_enum LEFT JOIN table_key USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_with_enum RIGHT JOIN table_key USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_with_enum FULL JOIN table_key USING (keycol) ORDER BY keycol;

SET join_algorithm = 'partial_merge';

SELECT keycol, enum_col FROM table_key LEFT JOIN table_with_enum USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_key FULL JOIN table_with_enum USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_key RIGHT JOIN table_with_enum USING (keycol) ORDER BY keycol;

SELECT keycol, enum_col FROM table_with_enum LEFT JOIN table_key USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_with_enum RIGHT JOIN table_key USING (keycol) ORDER BY keycol;
SELECT keycol, enum_col FROM table_with_enum FULL JOIN table_key USING (keycol) ORDER BY keycol;

DROP STREAM IF EXISTS table_key;
DROP STREAM IF EXISTS table_with_enum;

DROP DATABASE IF EXISTS test_01656;
