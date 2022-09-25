SELECT visitParamExtractUInt('"a":123', 'a');
SELECT visitParamExtractString('"a":"Hello"', 'a');
SELECT visitParamExtractRaw('"a":Hello}', 'a');

SELECT sum(ignore(visitParamExtractRaw(concat('{"a":', reinterpret_as_string(rand64())), 'a'))) FROM numbers(1000000);
