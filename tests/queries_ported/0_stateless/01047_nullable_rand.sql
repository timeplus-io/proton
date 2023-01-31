select to_type_name(rand(cast(4 as nullable(uint8))));
select to_type_name(rand_constant(CAST(4 as nullable(uint8))));
select to_type_name(rand(Null));
select to_type_name(rand_constant(Null));

select rand(cast(4 as nullable(uint8))) * 0;
select rand_constant(CAST(4 as nullable(uint8))) * 0;
select rand(Null) * 0;
select rand_constant(Null) * 0;
