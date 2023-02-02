CREATE STREAM low_card
(
    `lc` low_cardinality(string)
)
ENGINE = Join(ANY, LEFT, lc);

INSERT INTO low_card VALUES ( '1' );

SELECT * FROM low_card;
SELECT * FROM low_card WHERE lc = '1';
SELECT CAST(lc AS string) FROM low_card;

DROP STREAM low_card;
