SELECT '----- START -----';

SELECT '----- CONST -----';
select mortonEncode((1,2,3,1), 1,2,3,4);
select mortonDecode((1, 2, 3, 1), 4205569);
select mortonEncode((1,1), 65534, 65533);
select mortonDecode((1,1), 4294967286);
select mortonEncode(tuple(1), 4294967286);
select mortonDecode(tuple(1), 4294967286);
select mortonEncode(tuple(4), 128);
select mortonDecode(tuple(4), 2147483648);
select mortonEncode((4,4,4,4), 128, 128, 128, 128);

SELECT '----- (1,2,1,2) -----';
drop stream if exists morton_numbers_mask_02457;
create stream morton_numbers_mask_02457(
    n1 uint8,
    n2 uint8,
    n3 uint8,
    n4 uint8
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_02457
select n1.number, n2.number, n3.number, n4.number
from           numbers(256-16, 16) n1
    cross join numbers(256-16, 16) n2
    cross join numbers(256-16, 16) n3
    cross join numbers(256-16, 16) n4
;
drop stream if exists morton_numbers_mask_1_02457;
create stream morton_numbers_mask_1_02457(
    n1 uint64,
    n2 uint64,
    n3 uint64,
    n4 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_1_02457
select untuple(mortonDecode((1,2,1,2), mortonEncode((1,2,1,2), n1, n2, n3, n4)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_1_02457
)
except
(
    select * from morton_numbers_mask_02457
    intersect
    select * from morton_numbers_mask_1_02457
);
drop stream if exists morton_numbers_mask_02457;
drop stream if exists morton_numbers_mask_1_02457;

SELECT '----- (1,4) -----';
drop stream if exists morton_numbers_mask_02457;
create stream morton_numbers_mask_02457(
    n1 uint32,
    n2 uint8
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_02457
select n1.number, n2.number
from           numbers(pow(2, 32)-64, 64) n1
    cross join numbers(pow(2, 8)-64, 64) n2
;
drop stream if exists morton_numbers_mask_2_02457;
create stream morton_numbers_mask_2_02457(
    n1 uint64,
    n2 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_2_02457
select untuple(mortonDecode((1,4), mortonEncode((1,4), n1, n2)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_2_02457
)
except
(
    select * from morton_numbers_mask_02457
                      intersect
        select * from morton_numbers_mask_2_02457
);
drop stream if exists morton_numbers_mask_02457;
drop stream if exists morton_numbers_mask_2_02457;

SELECT '----- (1,1,2) -----';
drop stream if exists morton_numbers_mask_02457;
create stream morton_numbers_mask_02457(
    n1 uint16,
    n2 uint16,
    n3 uint8,
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_02457
select n1.number, n2.number, n3.number
from           numbers(pow(2, 16)-64, 64) n1
                   cross join numbers(pow(2, 16)-64, 64) n2
    cross join numbers(pow(2, 8)-64, 64) n3
;
drop stream if exists morton_numbers_mask_3_02457;
create stream morton_numbers_mask_3_02457(
    n1 uint64,
    n2 uint64,
    n3 uint64
)
    Engine=MergeTree()
    ORDER BY n1;

insert into morton_numbers_mask_3_02457
select untuple(mortonDecode((1,1,2), mortonEncode((1,1,2), n1, n2, n3)))
from morton_numbers_mask_02457;

(
    select * from morton_numbers_mask_02457
    union distinct
    select * from morton_numbers_mask_3_02457
)
except
(
    select * from morton_numbers_mask_02457
    intersect
    select * from morton_numbers_mask_3_02457
);
drop stream if exists morton_numbers_mask_02457;
drop stream if exists morton_numbers_mask_3_02457;

SELECT '----- END -----';
