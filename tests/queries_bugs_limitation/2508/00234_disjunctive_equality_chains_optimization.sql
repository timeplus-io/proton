SET query_mode='table';
SET asterisk_include_reserved_columns=false;

create stream IF NOT EXISTS foo_00234(id uint64) Engine=Memory;
INSERT INTO foo_00234(id) VALUES (0),(4),(1),(1),(3),(1),(1),(2),(2),(2),(1),(2),(3),(2),(1),(1),(2),(1),(1),(1),(3),(1),(2),(2),(1),(1),(3),(1),(2),(1),(1),(3),(2),(1),(1),(4),(0);

SELECT sleep(3);

SELECT sum(to_uint8(id = 3 OR id = 1 OR id = 2)) AS x, sum(to_uint8(id = 3 OR id = 1 OR id = 2)) AS x FROM foo_00234;
DROP STREAM foo_00234;
