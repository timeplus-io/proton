SET query_mode = 'table';
drop stream if exists data_01811;
drop stream if exists buffer_01811;

create stream data_01811 (key int) Engine=Memory();
/* Buffer with flush_rows=1000 */
create stream buffer_01811 (key int) Engine=Buffer(currentDatabase(), data_01811,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6,
    /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0
);

insert into buffer_01811 select * from numbers(10);
insert into buffer_01811 select * from numbers(10);

-- wait for background buffer flush
select sleep(3) format Null;
select count() from data_01811;

drop stream buffer_01811;
drop stream data_01811;
