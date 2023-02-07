CREATE STREAM fixed_string (id uint64, s fixed_string(256)) ENGINE = MergeTree() ORDER BY id;
CREATE STREAM suspicious_fixed_string (id uint64, s fixed_string(257)) ENGINE = MergeTree() ORDER BY id; -- { serverError 44 }
SET allow_suspicious_fixed_string_types = 1;
CREATE STREAM suspicious_fixed_string (id uint64, s fixed_string(257)) ENGINE = MergeTree() ORDER BY id;
