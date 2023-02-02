DROP STREAM IF EXISTS stream_with_enum_column_for_tsv_insert;

CREATE STREAM stream_with_enum_column_for_tsv_insert (
    Id int32,
    Value enum('ef' = 1, 'es' = 2)
) ENGINE=Memory();

SET input_format_tsv_enum_as_number = 1;

INSERT INTO stream_with_enum_column_for_tsv_insert FORMAT TSV 102	2
INSERT INTO stream_with_enum_column_for_tsv_insert FORMAT TabSeparatedRaw 103	1
SELECT * FROM stream_with_enum_column_for_tsv_insert ORDER BY Id;

SET input_format_tsv_enum_as_number = 0;

DROP STREAM IF EXISTS stream_with_enum_column_for_tsv_insert;
