DROP STREAM IF EXISTS alter_table;

CREATE STREAM alter_table (a uint8, b int16)
ENGINE = MergeTree
ORDER BY a;

ALTER STREAM alter_table
    MODIFY COLUMN `b` DateTime DEFAULT now(([NULL, NULL, NULL, [-2147483648], [NULL, NULL, NULL, NULL, NULL, NULL, NULL]] AND (1048576 AND NULL) AND (NULL AND 1048575 AND NULL AND -2147483649) AND NULL) IN (test_01103.t1_distr.id)); --{serverError 47}

SELECT 1;


DROP STREAM IF EXISTS alter_table;
