 

SELECT truncate(895, -16);
SELECT ( SELECT to_decimal128([], rowNumberInBlock()) ) , lcm('', [[(CAST(('>A') AS string))]]); -- { serverError 44 }
