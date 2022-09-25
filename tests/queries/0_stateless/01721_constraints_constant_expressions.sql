DROP STREAM IF EXISTS constraint_constant_number_expression;
create stream constraint_constant_number_expression
(
    id uint64,
    CONSTRAINT `c0` CHECK 1,
    CONSTRAINT `c1` CHECK 1 < 2,
    CONSTRAINT `c2` CHECK is_null(cast(NULL, 'nullable(uint8)'))
) ();

INSERT INTO constraint_constant_number_expression VALUES (1);

SELECT * FROM constraint_constant_number_expression;

DROP STREAM constraint_constant_number_expression;

DROP STREAM IF EXISTS constraint_constant_number_expression_non_uint8;
create stream constraint_constant_number_expression_non_uint8
(
    id uint64,
    CONSTRAINT `c0` CHECK to_uint64(1)
) ();

INSERT INTO constraint_constant_number_expression_non_uint8 VALUES (2); -- {serverError 1}

SELECT * FROM constraint_constant_number_expression_non_uint8;

DROP STREAM constraint_constant_number_expression_non_uint8;

DROP STREAM IF EXISTS constraint_constant_nullable_expression_that_contains_null;
create stream constraint_constant_nullable_expression_that_contains_null
(
    id uint64,
    CONSTRAINT `c0` CHECK null_if(1 % 2, 1)
) ();

INSERT INTO constraint_constant_nullable_expression_that_contains_null VALUES (3); -- {serverError 469}

SELECT * FROM constraint_constant_nullable_expression_that_contains_null;

DROP STREAM constraint_constant_nullable_expression_that_contains_null;
