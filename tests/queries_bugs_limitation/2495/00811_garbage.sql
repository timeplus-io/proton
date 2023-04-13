 

SELECT truncate(895, -16);
SELECT ( SELECT to_decimal128([], row_number_in_block()) ) , lcm('', [[(CAST(('>A') AS string))]]); -- { serverError 44 }
