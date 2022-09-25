-- Tags: long
--
-- Test for testing various read settings.
SET query_mode = 'table';
drop stream if exists data_02051;



create stream data_02051 (key int, value string)
engine=MergeTree()
order by key
settings
    index_granularity_bytes=0,
    /* to suppress "Table can't create parts with adaptive granularity, but settings ..." warning */
    min_bytes_for_wide_part=0
as select number, repeat(to_string(number), 5) from numbers(1e6);











select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;






drop stream data_02051;


create stream data_02051 (key int, value string)
engine=MergeTree()
order by key
settings
    index_granularity_bytes=10485760,
    /* to suppress "Table can't create parts with adaptive granularity, but settings ..." warning */
    min_bytes_for_wide_part=0
as select number, repeat(to_string(number), 5) from numbers(1e6);











select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='read',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='mmap',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_threadpool',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;













select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=0,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;










select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=0,
    read_priority=1,
    max_read_buffer_size=1048576
;







select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=0,
    max_read_buffer_size=1048576
;




select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=65505
;

select count(ignore(*)) from data_02051 settings
    min_bytes_to_use_direct_io=1,
    local_filesystem_read_method='pread_fake_async',
    local_filesystem_read_prefetch=1,
    read_priority=1,
    max_read_buffer_size=1048576
;






drop stream data_02051;


