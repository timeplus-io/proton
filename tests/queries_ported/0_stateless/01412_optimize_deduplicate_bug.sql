drop stream if exists tesd_dedupl;

create stream tesd_dedupl (x uint32, y uint32) engine = MergeTree order by x;
insert into tesd_dedupl values (1, 1);
insert into tesd_dedupl values (1, 1);

OPTIMIZE TABLE tesd_dedupl DEDUPLICATE;
select * from tesd_dedupl;

drop stream if exists tesd_dedupl;
