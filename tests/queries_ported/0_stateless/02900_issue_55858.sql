set precise_float_parsing = 1;

select cast('2023-01-01' as float64); -- { serverError 6 }
select cast('2023-01-01' as float32); -- { serverError 6 }
select to_Float32('2023-01-01'); -- { serverError 6 }
select to_Float64('2023-01-01'); -- { serverError 6 }
select to_Float32_Or_Zero('2023-01-01');
select to_Float64_Or_Zero('2023-01-01');
select to_Float32_Or_Null('2023-01-01');
select to_Float64_Or_Null('2023-01-01');
