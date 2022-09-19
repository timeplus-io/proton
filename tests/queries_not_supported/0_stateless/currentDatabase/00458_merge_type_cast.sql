SELECT ' uint32 | uint64 ';

DROP STREAM IF EXISTS u32;
DROP STREAM IF EXISTS u64;
DROP STREAM IF EXISTS merge_32_64;

create stream u32 (x uint32, y uint32 DEFAULT x) ;
create stream u64 (x uint64, y uint64 DEFAULT x) ;
create stream merge_32_64 (x uint64) ENGINE = Merge(currentDatabase(), '^u32|u64$');

INSERT INTO u32 (x) VALUES (1);
INSERT INTO u64 (x) VALUES (1);

INSERT INTO u32 (x) VALUES (4294967290);
INSERT INTO u64 (x) VALUES (4294967290);
--now inserts 3. maybe need out of range check?
--INSERT INTO u32 VALUES (4294967299);
INSERT INTO u64 (x) VALUES (4294967299);

select ' = 1:';
SELECT x FROM merge_32_64 WHERE x = 1;
select ' 1:';
SELECT x FROM merge_32_64 WHERE x IN (1);
select ' 4294967290:';
SELECT x FROM merge_32_64 WHERE x IN (4294967290);
select ' 4294967299:';
SELECT x FROM merge_32_64 WHERE x IN (4294967299);
--select ' -1: ';
--SELECT x FROM merge_32_64 WHERE x IN (-1);

DROP STREAM u32;
DROP STREAM u64;
DROP STREAM merge_32_64;


SELECT ' Int64 | uint64 ';

DROP STREAM IF EXISTS s64;
DROP STREAM IF EXISTS u64;
DROP STREAM IF EXISTS merge_s64_u64;

create stream s64 (x Int64) ;
create stream u64 (x uint64) ;
create stream merge_s64_u64 (x uint64) ENGINE = Merge(currentDatabase(), '^s64|u64$');

INSERT INTO s64 VALUES (1);
INSERT INTO s64 VALUES (-1);
INSERT INTO u64 VALUES (1);

select ' 1:';
SELECT x FROM merge_s64_u64 WHERE x IN (1);
select ' -1: ';
SELECT x FROM merge_s64_u64 WHERE x IN (-1);

DROP STREAM s64;
DROP STREAM u64;
DROP STREAM merge_s64_u64;


SELECT ' int32 | uint64 ';

DROP STREAM IF EXISTS one_00458;
DROP STREAM IF EXISTS two_00458;
DROP STREAM IF EXISTS merge_one_two;

create stream one_00458 (x int32) ;
create stream two_00458 (x uint64) ;
create stream merge_one_two (x uint64) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES (1);
INSERT INTO two_00458 VALUES (1);

INSERT INTO one_00458 VALUES (2147483650);
INSERT INTO two_00458 VALUES (2147483650);

SELECT * FROM merge_one_two WHERE x IN (1);
SELECT x FROM merge_one_two WHERE x IN (2147483650);
SELECT x FROM merge_one_two WHERE x IN (-1);


SELECT ' string | FixedString(16) ';

DROP STREAM IF EXISTS one_00458;
DROP STREAM IF EXISTS two_00458;
DROP STREAM IF EXISTS merge_one_two;

create stream one_00458 (x string) ;
create stream two_00458 (x FixedString(16)) ;
create stream merge_one_two (x string) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES ('1');
INSERT INTO two_00458 VALUES ('1');

SELECT * FROM merge_one_two WHERE x IN ('1');


SELECT ' DateTime | uint64 ';

DROP STREAM IF EXISTS one_00458;
DROP STREAM IF EXISTS two_00458;
DROP STREAM IF EXISTS merge_one_two;

create stream one_00458 (x DateTime) ;
create stream two_00458 (x uint64) ;
create stream merge_one_two (x uint64) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 VALUES (1);
INSERT INTO two_00458 VALUES (1);

SELECT * FROM merge_one_two WHERE x IN (1);


SELECT '  array(uint32) | array(uint64) ';

DROP STREAM IF EXISTS one_00458;
DROP STREAM IF EXISTS two_00458;
DROP STREAM IF EXISTS merge_one_two;

create stream one_00458 (x array(uint32), z string DEFAULT '', y array(uint32)) ;
create stream two_00458 (x array(uint64), z string DEFAULT '', y array(uint64)) ;
create stream merge_one_two (x array(uint64), z string, y array(uint64)) ENGINE = Merge(currentDatabase(), '^one_00458$|^two_00458$');

INSERT INTO one_00458 (x, y) VALUES ([1], [0]);
INSERT INTO two_00458 (x, y) VALUES ([1], [0]);
INSERT INTO one_00458 (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO two_00458 (x, y) VALUES ([4294967290], [4294967290]);
INSERT INTO one_00458 (x, y) VALUES ([4294967299], [4294967299]);
INSERT INTO two_00458 (x, y) VALUES ([4294967299], [4294967299]);

SELECT x, y FROM merge_one_two WHERE array_exists(_ -> _ IN (1), x);
SELECT x, y FROM merge_one_two WHERE array_exists(_ -> _ IN (4294967290), x);
SELECT x, y FROM merge_one_two WHERE array_exists(_ -> _ IN (4294967299), x);

DROP STREAM IF EXISTS one_00458;
DROP STREAM IF EXISTS two_00458;
DROP STREAM IF EXISTS merge_one_two;
