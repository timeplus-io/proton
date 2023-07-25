SELECT '----- START -----';
drop stream if exists morton_numbers_02457;
create stream morton_numbers_02457(
    n1 uint32,
    n2 uint32,
    n3 uint16,
    n4 uint16,
    n5 uint8,
    n6 uint8,
    n7 uint8,
    n8 uint8
)
    Engine=MergeTree()
    ORDER BY n1;

SELECT '----- CONST -----';
select morton_encode(1,2,3,4);
select morton_decode(4, 2149);
select morton_encode(65534, 65533);
select morton_decode(2, 4294967286);
select morton_encode(4294967286);
select morton_decode(1, 4294967286);

SELECT '----- 256, 8 -----';
insert into morton_numbers_02457
select n1.number, n2.number, n3.number, n4.number, n5.number, n6.number, n7.number, n8.number
from numbers(256-4, 4) as n1
    cross join numbers(256-4, 4) as n2
    cross join numbers(256-4, 4) as n3
    cross join numbers(256-4, 4) as n4
    cross join numbers(256-4, 4) as n5
    cross join numbers(256-4, 4) as n6
    cross join numbers(256-4, 4) as n7
    cross join numbers(256-4, 4) as n8
;
drop stream if exists morton_numbers_1_02457;
create stream morton_numbers_1_02457(
    n1 uint64,
    n2 uint64,
    n3 uint64,
    n4 uint64,
    n5 uint64,
    n6 uint64,
    n7 uint64,
    n8 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_1_02457
select untuple(morton_decode(8, morton_encode(n1, n2, n3, n4, n5, n6, n7, n8)))
from morton_numbers_02457;

(
    select * from morton_numbers_02457
    union distinct
    select * from morton_numbers_1_02457
)
except
(
    select * from morton_numbers_02457
    intersect
    select * from morton_numbers_1_02457
);
drop stream if exists morton_numbers_1_02457;

SELECT '----- 65536, 4 -----';
insert into morton_numbers_02457
select n1.number, n2.number, n3.number, n4.number, 0, 0, 0, 0
from numbers(pow(2, 16)-8,8) as n1
    cross join numbers(pow(2, 16)-8, 8) as n2
    cross join numbers(pow(2, 16)-8, 8) as n3
    cross join numbers(pow(2, 16)-8, 8) as n4
;

create stream morton_numbers_2_02457(
    n1 uint64,
    n2 uint64,
    n3 uint64,
    n4 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_2_02457
select untuple(morton_decode(4, morton_encode(n1, n2, n3, n4)))
from morton_numbers_02457;

(
    select n1, n2, n3, n4 from morton_numbers_02457
    union distinct
    select n1, n2, n3, n4 from morton_numbers_2_02457
)
except
(
    select n1, n2, n3, n4 from morton_numbers_02457
    intersect
    select n1, n2, n3, n4 from morton_numbers_2_02457
);
drop stream if exists morton_numbers_2_02457;

SELECT '----- 4294967296, 2 -----';
insert into morton_numbers_02457
select n1.number, n2.number, 0, 0, 0, 0, 0, 0
from numbers(pow(2, 32)-8,8) as n1
    cross join numbers(pow(2, 32)-8, 8) as n2
    cross join numbers(pow(2, 32)-8, 8) as n3
    cross join numbers(pow(2, 32)-8, 8) as n4
;

drop stream if exists morton_numbers_3_02457;
create stream morton_numbers_3_02457(
    n1 uint64,
    n2 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_3_02457
select untuple(morton_decode(2, morton_encode(n1, n2)))
from morton_numbers_02457;

(
    select n1, n2 from morton_numbers_3_02457
    union distinct
    select n1, n2 from morton_numbers_3_02457
)
except
(
    select n1, n2 from morton_numbers_3_02457
    intersect
    select n1, n2 from morton_numbers_3_02457
);
drop stream if exists morton_numbers_3_02457;

SELECT '----- END -----';
drop stream if exists morton_numbers_02457;
