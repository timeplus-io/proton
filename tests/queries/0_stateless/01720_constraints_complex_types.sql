SET allow_suspicious_low_cardinality_types = 1;

DROP STREAM IF EXISTS constraint_on_nullable_type;
create stream constraint_on_nullable_type
(
    `id` nullable(uint64),
    CONSTRAINT `c0` CHECK `id` = 1
)
();

INSERT INTO constraint_on_nullable_type VALUES (0); -- {serverError 469}
INSERT INTO constraint_on_nullable_type VALUES (1);

SELECT * FROM constraint_on_nullable_type;

DROP STREAM constraint_on_nullable_type;

DROP STREAM IF EXISTS constraint_on_low_cardinality_type;
create stream constraint_on_low_cardinality_type
(
    `id` low_cardinality(uint64),
    CONSTRAINT `c0` CHECK `id` = 2
)
;

INSERT INTO constraint_on_low_cardinality_type VALUES (0); -- {serverError 469}
INSERT INTO constraint_on_low_cardinality_type VALUES (2);

SELECT * FROM constraint_on_low_cardinality_type;

DROP STREAM constraint_on_low_cardinality_type;

DROP STREAM IF EXISTS constraint_on_low_cardinality_nullable_type;

create stream constraint_on_low_cardinality_nullable_type
(
    `id` low_cardinality(nullable(uint64)),
    CONSTRAINT `c0` CHECK `id` = 3
)
;

INSERT INTO constraint_on_low_cardinality_nullable_type VALUES (0); -- {serverError 469}
INSERT INTO constraint_on_low_cardinality_nullable_type VALUES (3);

SELECT * FROM constraint_on_low_cardinality_nullable_type;

DROP STREAM constraint_on_low_cardinality_nullable_type;
