SET query_mode = 'table';
drop stream if exists cdp_segments;
drop stream if exists cdp_customers;

create stream cdp_segments (seg_id string, mid_seqs aggregate_function(groupBitmap, uint32)) engine=ReplacingMergeTree() order by (seg_id);
create stream cdp_customers (mid string, mid_seq uint32) engine=ReplacingMergeTree() order by (mid_seq);
alter stream cdp_segments update mid_seqs = bitmapOr(mid_seqs, (select groupBitmapState(mid_seq) from cdp_customers where mid in ('6bf3c2ee-2b33-3030-9dc2-25c6c618d141'))) where seg_id = '1234567890';

drop stream cdp_segments;
drop stream cdp_customers;
