SELECT group_array(2 + 3) as number FROM numbers(10);
SELECT group_array('5'::uint8) as number FROM numbers(10);

SELECT group_array() as number FROM numbers(10); -- { serverError 36 }
SELECT group_array(NULL) as number FROM numbers(10); -- { serverError 36 }
SELECT group_array(NULL + NULL) as number FROM numbers(10); -- { serverError 36 }
SELECT group_array([]) as number FROM numbers(10); -- { serverError 36 }
SELECT group_array(throwIf(1)) as number FROM numbers(10); -- { serverError 395 }

-- Not the best error message, can be improved.
SELECT group_array as number as number FROM numbers(10); -- { serverError 47 }
