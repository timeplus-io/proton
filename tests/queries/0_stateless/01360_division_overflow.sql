select int_div(materialize(to_int32(1)), 0x100000000);
select int_div(materialize(to_int32(1)), -0x100000000);
select int_div(materialize(to_int32(1)), -9223372036854775808);
select materialize(to_int32(1)) % -9223372036854775808;
select value % -9223372036854775808 from (select to_int32(array_join([3, 5])) value);
