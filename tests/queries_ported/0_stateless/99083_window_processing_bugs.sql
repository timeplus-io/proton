--- https://github.com/timeplus-io/proton-enterprise/issues/9437
drop stream if exists 99083_stream;
create stream 99083_stream(id int);

select window_start, window_end from tumble(99083_stream, -1s); --- { serverError BAD_ARGUMENTS }
select window_start, window_end from hop(99083_stream, 1s, -1s); --- { serverError BAD_ARGUMENTS }
select window_start, window_end from session(99083_stream, -1s); --- { serverError BAD_ARGUMENTS }

select window_start, window_end from tumble(99083_stream, 0s); --- { serverError BAD_ARGUMENTS }
select window_start, window_end from hop(99083_stream, 0s, 5s); --- { serverError BAD_ARGUMENTS }
select window_start, window_end from session(99083_stream, 0s); --- { serverError BAD_ARGUMENTS }

select window_start, window_end from tumble(99083_stream, 2s) group by window_start, window_end emit periodic -1s; --- { serverError BAD_ARGUMENTS }

drop stream if exists 99083_stream;
