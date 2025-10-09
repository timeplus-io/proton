--- https://github.com/timeplus-io/proton-enterprise/issues/9601
drop view if exists 99087_view;
drop view if exists 99087_view2;
drop stream if exists 99087_stream;
create stream 99087_stream(id int, value string) settings shards=3, flush_threshold_count=1;

insert into 99087_stream(id, value) select number, to_string(number) from system.numbers limit 1000;
select sleep(2) format Null;

create materialized view 99087_view as select id as key, group_uniq_array(value), group_array((key, value)) from changelog(99087_stream, id) where _tp_time > earliest_ts() group by key emit delta periodic 100ms settings group_by_two_level_threshold=50;

insert into 99087_stream(id, value) select number, to_string(number) from system.numbers limit 1000;
select sleep(2) format Null;

create materialized view 99087_view2 as select id as key, group_uniq_array(value), group_array((key, value)) from changelog(99087_stream, id) where _tp_time > earliest_ts() group by key emit delta periodic 100ms settings group_by_two_level_threshold=50;
select sleep(2) format Null;

drop view if exists 99087_view;
drop view if exists 99087_view2;
drop stream if exists 99087_stream;
