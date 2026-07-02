drop stream if exists 99250_random_stream_map;

create random stream 99250_random_stream_map
(
    `m` map(string, int32)
);

select to_type_name(m) from table(99250_random_stream_map) limit 1;

drop stream if exists 99250_random_stream_map;
