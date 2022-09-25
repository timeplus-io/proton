SELECT parseDateTimeBestEffort('<Empty>'); -- { serverError 41 }
SELECT parse_datetime_best_effort_or_null('<Empty>');
SELECT parse_datetime_best_effort_or_zero('<Empty>', 'UTC');

SELECT parseDateTime64BestEffort('<Empty>'); -- { serverError 41 }
SELECT parseDateTime64BestEffortOrNull('<Empty>');
SELECT parseDateTime64BestEffortOrZero('<Empty>', 0, 'UTC');

SET date_time_input_format = 'best_effort';
SELECT to_datetime('<Empty>'); -- { serverError 41 }
SELECT toDateTimeOrNull('<Empty>');
SELECT toDateTimeOrZero('<Empty>', 'UTC');
