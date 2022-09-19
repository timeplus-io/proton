select to_type_name(rand(cast(4 as Nullable(uint8))));
select to_type_name(randConstant(CAST(4 as Nullable(uint8))));
select to_type_name(rand(Null));
select to_type_name(randConstant(Null));

select rand(cast(4 as Nullable(uint8))) * 0;
select randConstant(CAST(4 as Nullable(uint8))) * 0;
select rand(Null) * 0;
select randConstant(Null) * 0;
