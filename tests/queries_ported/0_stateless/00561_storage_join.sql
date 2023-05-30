SET query_mode = 'table';

SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

drop stream IF EXISTS joinbug;

create stream joinbug (
  event_date date MATERIALIZED to_date(created, 'Europe/Moscow'),
  id uint64,
  id2 uint64,
  val uint64,
  val2 int32,
  created uint64
) ENGINE = MergeTree(event_date, (id, id2), 8192);

insert into joinbug (id, id2, val, val2, created) values (1,11,91,81,123456), (2,22,92,82,123457);

drop stream IF EXISTS joinbug_join;

create stream joinbug_join (
  id uint64,
  id2 uint64,
  val uint64,
  val2 int32,
  created uint64
) ENGINE = Join(SEMI, LEFT, id2);

insert into joinbug_join (id, id2, val, val2, created)
select id, id2, val, val2, created
from joinbug;

select * from joinbug;

select id, id2, val, val2, created
from ( SELECT to_uint64(array_join(range(50))) AS id2 ) as js1
SEMI LEFT JOIN joinbug_join using id2;

-- type conversion
SELECT * FROM ( SELECT to_uint32(11) AS id2 ) AS js1 SEMI LEFT JOIN joinbug_join USING (id2);

-- can't convert right side in case on storage join
SELECT * FROM ( SELECT to_int64(11) AS id2 ) AS js1 SEMI LEFT JOIN joinbug_join USING (id2); -- { serverError 53 }

DROP STREAM joinbug;
DROP STREAM joinbug_join;
