SELECT substring_utf8(materialize(''), -9223372036854775808) FROM numbers(7);
