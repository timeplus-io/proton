-- Tags: no-parallel

drop database if exists test_01600;
create database test_01600;

create stream test_01600.base
(
`id` uint64,
`id2` uint64,
`d` uint64,
`value` uint64
)
ENGINE=MergeTree()
PARTITION BY d
ORDER BY (id,id2,d);

create stream test_01600.derived1
(
    `id1` uint64,
    `d1` uint64,
    `value1` uint64
)
ENGINE = MergeTree()
PARTITION BY d1
ORDER BY (id1, d1)
;

create stream test_01600.derived2
(
    `id2` uint64,
    `d2` uint64,
    `value2` uint64
)
ENGINE = MergeTree()
PARTITION BY d2
ORDER BY (id2, d2)
;

select 
base.id as `base.id`,
derived2.value2 as `derived2.value2`,
derived1.value1 as `derived1.value1`
from test_01600.base as base 
left join test_01600.derived2 as derived2 on base.id2 = derived2.id2
left join test_01600.derived1 as derived1 on base.id = derived1.id1;


SELECT
    base.id AS `base.id`,
    derived1.value1 AS `derived1.value1`
FROM test_01600.base AS base
LEFT JOIN test_01600.derived1 AS derived1 ON base.id = derived1.id1;

drop database test_01600;
