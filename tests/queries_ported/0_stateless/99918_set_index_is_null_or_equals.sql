-- Regression test for proton-enterprise#11936:
-- `col IS NULL OR col = '...'` on a stream with a set skip index must not
-- prune granules whose set entry contains NULL.

drop stream if exists test_set_idx_null;

create stream test_set_idx_null
(
    algoid nullable(string),
    INDEX idx1 algoid TYPE set(0) GRANULARITY 2
) settings flush_threshold_count=1;

insert into test_set_idx_null(algoid) select null as algoid from numbers(10000);
insert into test_set_idx_null(algoid) select '' as algoid from numbers(10000);

select sleep(2) format Null;

select count() from table(test_set_idx_null) where algoid is null or algoid = '';
select count() from table(test_set_idx_null) where algoid is null;
select count() from table(test_set_idx_null) where algoid = '';

drop stream if exists test_set_idx_null;
