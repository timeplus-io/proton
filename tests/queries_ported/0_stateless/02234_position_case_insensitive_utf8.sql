SELECT position_case_insensitive_utf8('Hello', materialize('%\xF0%'));
SELECT DISTINCT position_case_insensitive_utf8(materialize('Hello'), '%\xF0%') FROM numbers(1000);
