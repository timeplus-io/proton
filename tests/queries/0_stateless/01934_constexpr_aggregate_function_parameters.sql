SELECT group_array(2 + 3)(number) FROM numbers(10);
SELECT group_array('5'::uint8)(number) FROM numbers(10);

SELECT group_array()(number) FROM numbers(10); -- { serverError 36 }
SELECT group_array(NULL)(number) FROM numbers(10); -- { serverError 36 }
SELECT group_array(NULL + NULL)(number) FROM numbers(10); -- { serverError 36 }
SELECT group_array([])(number) FROM numbers(10); -- { serverError 36 }
SELECT group_array(throwIf(1))(number) FROM numbers(10); -- { serverError 395 }

-- Not the best error message, can be improved.
SELECT group_array(number)(number) FROM numbers(10); -- { serverError 47 }
