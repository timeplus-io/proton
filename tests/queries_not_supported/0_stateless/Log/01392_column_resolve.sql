-- Tags: no-parallel

DROP DATABASE IF EXISTS test_01392;
CREATE DATABASE test_01392;

CREATE STREAM test_01392.tableConversion (conversionId string, value nullable(Double)) ENGINE = Log();
CREATE STREAM test_01392.tableClick (clickId string, conversionId string, value nullable(Double)) ENGINE = Log();
CREATE STREAM test_01392.leftjoin (id string) ENGINE = Log();

INSERT INTO test_01392.tableConversion(conversionId, value) VALUES ('Conversion 1', 1);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 1', 'Conversion 1', 14);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 2', 'Conversion 1', 15);
INSERT INTO test_01392.tableClick(clickId, conversionId, value) VALUES ('Click 3', 'Conversion 1', 16);

SELECT
    conversion.conversionId AS myConversionId,
    click.clickId AS myClickId,
    click.myValue AS myValue
FROM (
    SELECT conversionId, value as myValue
    FROM test_01392.tableConversion
) AS conversion
INNER JOIN (
    SELECT clickId, conversionId, value as myValue
    FROM test_01392.tableClick
) AS click ON click.conversionId = conversion.conversionId
LEFT JOIN (
    SELECT * FROM test_01392.leftjoin
) AS dummy ON (dummy.id = conversion.conversionId)
ORDER BY myValue;

DROP STREAM test_01392.tableConversion;
DROP STREAM test_01392.tableClick;
DROP STREAM test_01392.leftjoin;

DROP DATABASE test_01392;
