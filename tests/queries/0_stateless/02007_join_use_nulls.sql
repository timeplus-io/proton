SET join_use_nulls = 1;

SELECT *, d.* FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;

SELECT id, to_type_name(id), value, to_type_name(value), d.values, to_type_name(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.values, to_type_name(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.id, to_type_name(d.id) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.values, to_type_name(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.id, to_type_name(d.id) , d.values, to_type_name(d.values) FROM ( SELECT 1 AS id, 2 AS value ) a SEMI LEFT JOIN ( SELECT 1 AS id, 3 AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.values, to_type_name(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
SELECT id, to_type_name(id), value, to_type_name(value), d.id, to_type_name(d.id) , d.values, to_type_name(d.values) FROM ( SELECT toLowCardinality(1) AS id, toLowCardinality(2) AS value ) a SEMI LEFT JOIN ( SELECT toLowCardinality(1) AS id, toLowCardinality(3) AS values ) AS d USING id;
