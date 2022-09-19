DROP STREAM IF EXISTS type_names;
DROP STREAM IF EXISTS values_template;
DROP STREAM IF EXISTS values_template_nullable;
DROP STREAM IF EXISTS values_template_fallback;

SET input_format_null_as_default = 0;

create stream type_names (n uint8, s1 string, s2 string, s3 string) ENGINE=Memory;
create stream values_template (d date, s string, u uint8, i int64, f float64, a array(uint8)) ;
create stream values_template_nullable (d date, s Nullable(string), u Nullable(uint8), a array(Nullable(Float32))) ;
create stream values_template_fallback (n uint8) ;

SET input_format_values_interpret_expressions = 0;

-- checks type deduction
INSERT INTO type_names VALUES (1, to_type_name([1, 2]), to_type_name((256, -1, 3.14, 'str', [1, -1])), to_type_name([(1, [256]), (256, [1, 2])])), (2, to_type_name([1, -1]), to_type_name((256, -1, 3, 'str', [1, 2])), to_type_name([(256, []), (1, [])]));

--(1, lower(replaceAll(_STR_1, 'o', 'a')), _NUM_1 + _NUM_2 + _NUM_3, round(_NUM_4 / _NUM_5), _NUM_6 * CAST(_STR_7, 'int8'), _ARR_8);
-- _NUM_1: uint64 -> int64 -> uint64
-- _NUM_4: int64 -> uint64
-- _NUM_5: float64 -> int64
INSERT INTO values_template VALUES ((1), lower(replaceAll('Hella', 'a', 'o')), 1 + 2 + 3, round(-4 * 5.0), nan / CAST('42', 'int8'), reverse([1, 2, 3])), ((2), lower(replaceAll('Warld', 'a', 'o')), -4 + 5 + 6, round(18446744073709551615 * 1e-19), 1.0 / CAST('0', 'int8'), reverse([])), ((3), lower(replaceAll('Test', 'a', 'o')), 3 + 2 + 1, round(9223372036854775807 * -1), 6.28  / CAST('2', 'int8'), reverse([4, 5])), ((4), lower(replaceAll('Expressians', 'a', 'o')), 6 + 5 + 4, round(1 * -9223372036854775807), 127.0 / CAST('127', 'int8'), reverse([6, 7, 8, 9, 0]));

INSERT INTO values_template_nullable VALUES ((1), lower(replaceAll('Hella', 'a', 'o')), 1 + 2 + 3, arraySort(x -> assumeNotNull(x), [null, NULL])),   ((2), lower(replaceAll('Warld', 'b', 'o')), 4 - 5 + 6, arraySort(x -> assumeNotNull(x), [+1, -1, Null])),   ((3), lower(replaceAll('Test', 'c', 'o')), 3 + 2 - 1, arraySort(x -> assumeNotNull(x), [1, nUlL, 3.14])),   ((4), lower(replaceAll(null, 'c', 'o')), 6 + 5 - null, arraySort(x -> assumeNotNull(x), [3, 2, 1]));

INSERT INTO values_template_fallback VALUES (1 + x); -- { clientError 62 }
INSERT INTO values_template_fallback VALUES (abs(functionThatDoesNotExists(42))); -- { clientError 46 }
INSERT INTO values_template_fallback VALUES ([1]); -- { clientError 43 }

INSERT INTO values_template_fallback VALUES (CAST(1, 'uint8')), (CAST('2', 'uint8'));
SET input_format_values_accurate_types_of_literals = 0;

INSERT INTO type_names VALUES (3, to_type_name([1, 2]), to_type_name((256, -1, 3.14, 'str', [1, -1])), to_type_name([(1, [256]), (256, [1, 2])])), (4, to_type_name([1, -1]), to_type_name((256, -1, 3, 'str', [1, 2])), to_type_name([(256, []), (1, [])]));
SET input_format_values_interpret_expressions = 1;
INSERT INTO values_template_fallback VALUES (1 + 2), (3 + +04), (5 + 6);
INSERT INTO values_template_fallback VALUES (+020), (+030), (+040);

SELECT * FROM type_names ORDER BY n;
SELECT * FROM values_template ORDER BY d;
SELECT * FROM values_template_nullable ORDER BY d;
SELECT * FROM values_template_fallback ORDER BY n;
DROP STREAM type_names;
DROP STREAM values_template;
DROP STREAM values_template_nullable;
DROP STREAM values_template_fallback;
