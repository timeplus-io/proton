DROP STREAM IF EXISTS tableCommon;
DROP STREAM IF EXISTS tableTrees;
DROP STREAM IF EXISTS tableFlowers;

create stream tableCommon (`key` FixedString(15), `value` Nullable(int8))  ();
create stream tableTrees (`key` FixedString(15), `name` Nullable(int8), `name2` Nullable(int8))  ();
create stream tableFlowers (`key` FixedString(15), `name` Nullable(int8))  ();

SELECT * FROM (
    SELECT common.key, common.value, trees.name, trees.name2
    FROM (
	SELECT *
	FROM tableCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM tableTrees
    ) trees ON (common.key = trees.key)
)
UNION ALL
(
    SELECT common.key, common.value, 
    null as name, null as name2 
    
    FROM (
	SELECT *
	FROM tableCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM tableFlowers
    ) flowers ON (common.key = flowers.key)
);

SELECT * FROM (
    SELECT common.key, common.value, trees.name, trees.name2
    FROM (
	SELECT *
	FROM tableCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM tableTrees
    ) trees ON (common.key = trees.key)
)
UNION ALL
(
    SELECT common.key, common.value, 
    flowers.name, null as name2

    FROM (
	SELECT *
	FROM tableCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM tableFlowers
    ) flowers ON (common.key = flowers.key)
);

DROP STREAM IF EXISTS tableCommon;
DROP STREAM IF EXISTS tableTrees;
DROP STREAM IF EXISTS tableFlowers;
