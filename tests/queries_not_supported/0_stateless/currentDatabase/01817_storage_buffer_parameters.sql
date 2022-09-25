SET query_mode = 'table';
drop stream if exists data_01817;
drop stream if exists buffer_01817;

create stream data_01817 (key int) Engine=Null();

-- w/ flush_*
create stream buffer_01817 (key int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6,
    /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0
);
drop stream buffer_01817;

-- w/o flush_*
create stream buffer_01817 (key int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6
);
drop stream buffer_01817;

-- not enough args
create stream buffer_01817 (key int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0      /* max_bytes= 4e6  */
); -- { serverError 42 }
-- too much args
create stream buffer_01817 (key int) Engine=Buffer(currentDatabase(), data_01817,
    /* num_layers= */ 1,
    /* min_time= */   1,     /* max_time= */  86400,
    /* min_rows= */   1e9,   /* max_rows= */  1e6,
    /* min_bytes= */  0,     /* max_bytes= */ 4e6,
    /* flush_time= */ 86400, /* flush_rows= */ 10, /* flush_bytes= */0,
    0
); -- { serverError 42 }

drop stream data_01817;
