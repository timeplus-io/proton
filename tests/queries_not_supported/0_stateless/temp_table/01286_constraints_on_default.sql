DROP STREAM IF EXISTS default_constraints;
create stream default_constraints
(
    x uint8,
    y uint8 DEFAULT x + 1,
    CONSTRAINT c CHECK y < 5
) ;

INSERT INTO default_constraints (x) SELECT number FROM system.numbers LIMIT 5; -- { serverError 469 }
INSERT INTO default_constraints (x) VALUES (0),(1),(2),(3),(4); -- { serverError 469 }

SELECT y, throwIf(NOT y < 5) FROM default_constraints;
SELECT count() FROM default_constraints;

DROP STREAM default_constraints;


CREATE TEMPORARY STREAM default_constraints
(
    x uint8,
    y uint8 DEFAULT x + 1,
    CONSTRAINT c CHECK y < 5
);

INSERT INTO default_constraints (x) SELECT number FROM system.numbers LIMIT 5; -- { serverError 469 }
INSERT INTO default_constraints (x) VALUES (0),(1),(2),(3),(4); -- { serverError 469 }

SELECT y, throwIf(NOT y < 5) FROM default_constraints;
SELECT count() FROM default_constraints;
