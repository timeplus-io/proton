create stream table_2009_part (`i` int64, `d` date, `s` string) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY i;

ALTER STREAM table_2009_part ATTACH PARTITION tuple(array_join([0, 1])); -- {serverError 36}
ALTER STREAM table_2009_part ATTACH PARTITION tuple(toYYYYMM(to_date([array_join([array_join([array_join([array_join([3, materialize(NULL), array_join([1025, materialize(NULL), materialize(NULL)]), NULL])])]), materialize(NULL)])], NULL))); -- {serverError 36}
