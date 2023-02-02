SELECT parse_datetime_best_effort('<Empty>'); -- { serverError 41 }
SELECT parse_datetime_best_effort_or_null('<Empty>');
SELECT parse_datetime_best_effort_or_zero('<Empty>', 'UTC');

SELECT parse_datetime64_best_effort('<Empty>'); -- { serverError 41 }
SELECT parse_datetime64_best_effort_or_null('<Empty>');
SELECT parse_datetime64_best_effort_or_zero('<Empty>', 0, 'UTC');

SET date_time_input_format = 'best_effort';
SELECT to_datetime('<Empty>'); -- { serverError 41 }
SELECT to_datetime_or_null('<Empty>');
SELECT to_datetime_or_zero('<Empty>', 'UTC');
