DROP STREAM IF EXISTS streamCommon;
DROP STREAM IF EXISTS streamTrees;
DROP STREAM IF EXISTS streamFlowers;

CREATE STREAM streamCommon (`key` fixed_string(15), `value` nullable(int8)) ENGINE = Log();
CREATE STREAM streamTrees (`key` fixed_string(15), `name` nullable(int8), `name2` nullable(int8)) ENGINE = Log();
CREATE STREAM streamFlowers (`key` fixed_string(15), `name` nullable(int8)) ENGINE = Log();

SELECT * FROM (
    SELECT common.key, common.value, trees.name, trees.name2
    FROM (
	SELECT *
	FROM streamCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM streamTrees
    ) trees ON (common.key = trees.key)
)
UNION ALL
(
    SELECT common.key, common.value, 
    null as name, null as name2 
    
    FROM (
	SELECT *
	FROM streamCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM streamFlowers
    ) flowers ON (common.key = flowers.key)
);

SELECT * FROM (
    SELECT common.key, common.value, trees.name, trees.name2
    FROM (
	SELECT *
	FROM streamCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM streamTrees
    ) trees ON (common.key = trees.key)
)
UNION ALL
(
    SELECT common.key, common.value, 
    flowers.name, null as name2

    FROM (
	SELECT *
	FROM streamCommon
    ) as common
    INNER JOIN (
	SELECT *
	FROM streamFlowers
    ) flowers ON (common.key = flowers.key)
);

DROP STREAM IF EXISTS streamCommon;
DROP STREAM IF EXISTS streamTrees;
DROP STREAM IF EXISTS streamFlowers;
