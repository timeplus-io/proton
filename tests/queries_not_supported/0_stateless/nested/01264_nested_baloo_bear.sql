DROP STREAM IF EXISTS LOG_T;

create stream LOG_T
(
    `fingerprint` uint64, 
    `fields` nested(
    name LowCardinality(string), 
    value string)
)
ENGINE = MergeTree
ORDER BY fingerprint;

SELECT
    fields.name,
    fields.value
FROM
(
    SELECT
        fields.name,
        fields.value
    FROM LOG_T
)
WHERE has(['node'], fields.value[indexOf(fields.name, 'ProcessName')]);

INSERT INTO LOG_T VALUES (123, ['Hello', 'ProcessName'], ['World', 'node']);

SELECT
    fields.name,
    fields.value
FROM
(
    SELECT
        fields.name,
        fields.value
    FROM LOG_T
)
WHERE has(['node'], fields.value[indexOf(fields.name, 'ProcessName')]);

DROP STREAM LOG_T;
