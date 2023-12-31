DROP STREAM IF EXISTS data_01515;
CREATE STREAM data_01515
(
    key int,
    d1 int,
    d1_null nullable(int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assume_not_null(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data_01515 VALUES (1, 2, 3);

SELECT * FROM data_01515;
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- { serverError 6 }
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }

SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices=' d1_idx ';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_idx  ';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx,d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx,d1_idx,,'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_null_idx,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  `d1_null_idx`,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_null_idx  '; -- { serverError 277 }

SELECT * FROM data_01515 WHERE d1_null = 0 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE assume_not_null(d1_null) = 0 SETTINGS force_data_skipping_indices='d1_null_idx';

DROP STREAM data_01515;
