SET query_mode = 'table';
drop stream if exists set_index;

create stream set_index (a int32, b int32, INDEX b_set b type set(0) granularity 1) engine MergeTree order by tuple();
insert into set_index values (1, 2);

select b from set_index where a = 1 and a = 1 and b = 1 settings force_data_skipping_indices = 'b_set', optimize_move_to_prewhere=0;
