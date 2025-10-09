drop stream if exists 99104_stream;
create stream 99104_stream(i int, t datetime64(3, 'UTC')) settings flush_threshold_count=1;

select sleep(1) format Null;
insert into 99104_stream(i) select number as i from system.numbers limit 10000;

select sleep(2) format Null;
--- backfilled time column `t` from MergeTree is ColumnSparse, and all rows are default value
select window_start, window_end, count() from tumble(99104_stream, t, 2s) group by window_start, window_end limit 1 emit timeout 2s settings seek_to='earliest';
select window_start, window_end, max(t) from hop(99104_stream, t, 2s, 10s) group by window_start, window_end limit 1 emit timeout 2s settings seek_to='earliest';
select window_start, window_end, count(), max(t), min(t) from session(99104_stream, t, 30s) group by window_start, window_end limit 1 emit timeout 2s settings seek_to='earliest';

drop stream if exists 99104_stream;
