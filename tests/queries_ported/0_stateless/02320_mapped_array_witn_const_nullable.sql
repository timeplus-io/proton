-- Tags: no-backward-compatibility-check

select array_map(x -> to_nullable(1), range(number)) from numbers(3);
select array_filter(x -> to_nullable(1), range(number)) from numbers(3);
select array_map(x -> to_nullable(0), range(number)) from numbers(3);
select array_filter(x -> to_nullable(0), range(number)) from numbers(3);
select array_map(x -> NULL::nullable(uint8), range(number)) from numbers(3);
select array_filter(x -> NULL::nullable(uint8), range(number)) from numbers(3);

