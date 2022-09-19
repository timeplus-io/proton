SET send_logs_level = 'fatal';

DROP STREAM IF EXISTS Issue_2231_Invalid_Nested_Columns_Size;
create stream Issue_2231_Invalid_Nested_Columns_Size (
    date date,
    NestedColumn nested(
        ID    int32,
        Count Int64
    )
) Engine = MergeTree 
    PARTITION BY tuple()
    ORDER BY date;

INSERT INTO Issue_2231_Invalid_Nested_Columns_Size VALUES (today(), [2,2], [1]), (today(), [2,2], [1, 1]); -- { serverError 190 }

SELECT * FROM Issue_2231_Invalid_Nested_Columns_Size;
DROP STREAM Issue_2231_Invalid_Nested_Columns_Size;
